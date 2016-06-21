open System
open System.Threading
open Akka.FSharp
open Akka.Actor
type term = int
type id = int
type logIndex = int
type commitIndex = int
type command = int
type entry = {
    term: term
    command: command
}
type AppendEntries = {
    term: term
    leaderId: id
    prevLogIndex: logIndex
    prevLogTerm: term
    entries: List<entry>
    leaderCommit: commitIndex
}
type AppendEntriesResult = {
    term: term
    success: bool
}
type persistentState = {
    currentTerm: term
    votedFor: Option<id*term>
    log: List<entry>
}
type volatileState = {
    commitIndex: logIndex
    lastApplied: logIndex
}
type leaderState = {
    nextIndex: Map<id, logIndex>
    matchIndex: Map<id, logIndex>
}
type data = {
    persistentState: persistentState
    volatileState: volatileState
    leaderState: Option<leaderState>
    electionState: Option<Map<id, bool>>
}
type RequestVote = {
    term: term
    candidateId: id
    lastLogIndex: logIndex
    lastLogTerm: term
}
type RequestVoteResult = {
    term: term
    voteGranted: bool
    id: id
}
type state = Leader | Candidate | Follower | Init
type MyActor() as x =
    inherit Actor()
    let mutable cancellationSource = new CancellationTokenSource()
    do
        x.setTimeout()
    member x.persistentState = {currentTerm = 1; votedFor = None; log = []}
    
    member x.setTimeout = fun () ->
        let timeoutWorkflow = async {
            printf "wait start\n"
            do! Async.Sleep(3000)
            printf "wait done\n"
        }
        cancellationSource <- new CancellationTokenSource()
        Async.Start (timeoutWorkflow, cancellationSource.Token)
    
        
    override x.OnReceive message =
        let tid = 1
        match message with
        | :? string as msg ->
            printfn "Hello %s from %s at #%d thread" msg "sd" tid
            cancellationSource.Cancel()
            printfn "Hello %s from %s at #%d thread" msg "sd" tid
            x.setTimeout()
        | _ ->  failwith "unknown message"

type ElectionTimeout =
    new() = {}

type Rafter(id: id, timeout: int, actorSelection: ActorSelection, servers: int) as x =
    inherit FSM<state, data>()
    //todo retry rpc if no response
    let electionTimeout = Nullable(TimeSpan.FromSeconds(float(timeout)))
    let persistentStateDefault() =
        {currentTerm = 1; votedFor = None; log = []}//todo 1 or 0?
    let volatileStateDefault() =
        {commitIndex = 0; lastApplied = 0}
    let initLeaderState(d: data) =
        {
            leaderState.nextIndex = Map([1..servers] |> List.map (fun i -> (i, d.persistentState.log.Length + 1)))
            leaderState.matchIndex = Map([1..servers] |> List.map (fun i -> (i, 0)))
        }
    let defaultData() =
        {persistentState = persistentStateDefault(); volatileState = volatileStateDefault(); leaderState = None; electionState = None}
    let lastLogTerm(d: data) =
        if d.persistentState.log.IsEmpty then 0 else d.persistentState.log.[d.persistentState.log.Length - 1].term
    let beginElection(d: data) =
        printfn "%d begins election" id
        let newData = { d with persistentState = { d.persistentState with currentTerm = d.persistentState.currentTerm + 1; votedFor = Some(id, d.persistentState.currentTerm + 1) }; electionState = Some(Map([id, true]))}
        actorSelection <! {
            term = newData.persistentState.currentTerm
            candidateId = id
            lastLogIndex = List.length d.persistentState.log
            lastLogTerm = lastLogTerm d
        }
        newData
    
    do
        x.StartWith(Init, defaultData(), Nullable(TimeSpan.Zero))
        x.When(Init, (fun (event) ->
            printfn "%d State: %A, Event: %A" id Init event.FsmEvent
            match (event.FsmEvent, event.StateData) with
            | (:? FSMBase.StateTimeout, d: data) -> x.GoTo(Follower)
            | (_, d: data) -> x.Stay()
        ))
        x.When(Follower, (fun (event) ->
            printfn "%d State: %A, Event: %A" id Follower event.FsmEvent
            match (event.FsmEvent, event.StateData) with
            | (:? ElectionTimeout, d: data) -> x.GoTo(Candidate, beginElection(d))
            | (:? RequestVote as r, d: data) -> x.HandleRequestVote(Follower, d, r) |> x.GoTo
            | (:? RequestVoteResult as r, d: data) ->
                let currentTerm = max r.term d.persistentState.currentTerm
                x.GoTo(Follower, {d with persistentState = {d.persistentState with currentTerm = currentTerm}})
            | (:? AppendEntries as r, d: data) -> x.HandleAppendEntries(Follower, d, r) |> x.GoTo
            | (_, d: data) -> x.Stay()
        ))
        x.When(Candidate, (fun (event) ->
            printfn "%d State: %A, Event: %A" id Candidate event.FsmEvent
            match (event.FsmEvent, event.StateData) with
            | (:? ElectionTimeout, d: data) -> x.GoTo(Candidate, beginElection(d))
            | (:? RequestVote as r, d: data) -> x.HandleRequestVote(Candidate, d, r) |> x.GoTo
            | (:? RequestVoteResult as r, d: data) ->
                printfn "vote granted: %A" r.voteGranted
                if r.term > d.persistentState.currentTerm then
                    x.GoTo(Follower, {
                        d with
                            persistentState = {d.persistentState with currentTerm = r.term}
                            electionState = None
                    })
                else if r.term = d.persistentState.currentTerm then
                    let electionState = Map.add r.id r.voteGranted d.electionState.Value
                    let votes = (Map.filter (fun (k: id)(v: bool) -> v) electionState).Count
                    if votes > servers / 2 then
                        printfn "imma leader %d" id
                        
                        x.GoTo(Leader, {d with electionState = None; leaderState = Some(initLeaderState d)})
                    else
                        x.GoTo(Candidate, {d with electionState = Some(electionState)})
                else
                    x.Stay()
            | (:? AppendEntries as r, d: data) -> x.HandleAppendEntries(Candidate, d, r) |> x.GoTo
            | (_, d: data) -> x.Stay()
        ))
        x.When(Leader, (fun (event) ->
            printfn "%d State: %A, Event: %A" id Leader event.FsmEvent
            match (event.FsmEvent, event.StateData) with
            | (:? FSMBase.StateTimeout, d: data) ->
                x.EmitAppendEntries(d, true)
                x.Stay()
            | (:? RequestVote as r, d: data) -> x.HandleRequestVote(Leader, d, r) |> x.GoTo
            | (:? RequestVoteResult as r, d: data) ->
                if r.term > d.persistentState.currentTerm then
                    x.GoTo(Follower, {
                        d with
                            persistentState = {d.persistentState with currentTerm = r.term}
                            leaderState = None
                    })
                else
                    x.Stay()
            | (:? AppendEntries as r, d: data) -> x.HandleAppendEntries(Leader, d, r) |> x.GoTo
            | (:? command as r, d: data) ->
                let newData = {d with persistentState = {d.persistentState with log = (d.persistentState.log @ [{term = d.persistentState.currentTerm; command = r}])}}
                x.EmitAppendEntries(newData, false)
                x.GoTo(Leader, newData)
            | (_, d: data) -> x.Stay()
        ))
        x.OnTransition(fun s1 s2 ->
            printfn "%d %A -> %A" id s1 s2
            let electionTimeoutName = "election"
            let startElectionTimeout() = 
                printfn "Starting election timeout"
                x.SetTimer(electionTimeoutName, ElectionTimeout(), TimeSpan.FromSeconds(float(timeout)), true)

            match (s1, s2) with
            | (Init, Follower) -> startElectionTimeout()
            | (Candidate, Follower) ->
                x.CancelTimer(electionTimeoutName)
                startElectionTimeout()
            | (Candidate, Leader) ->
                //todo noop to log
                x.CancelTimer(electionTimeoutName)
                x.EmitAppendEntries(x.NextStateData, true)
            | (Leader, Follower) -> startElectionTimeout()
            | (_) -> ()
            
        )
        x.Initialize()
    member x.HandleRequestVote(s: state, d: data, r: RequestVote): (state * data) =
        if false && r.candidateId = id then
            (s, d)
        else
            let lastLogTerm = lastLogTerm d
            let result =
                if r.term < d.persistentState.currentTerm
                    then false
                else
                (match d.persistentState.votedFor with
                    | Some(idd, term) -> r.term > term || idd = r.candidateId
                    | None -> true) && (r.lastLogTerm > lastLogTerm ||
                        r.lastLogTerm = lastLogTerm && r.lastLogIndex >= d.persistentState.log.Length)
            let newState = if r.term > d.persistentState.currentTerm then Follower else s
            let newTerm = max r.term  d.persistentState.currentTerm
            x.Sender <! {term = newTerm; voteGranted = result; id = id}
            let newData = {d with persistentState = { d.persistentState with votedFor = (if result then Some(r.candidateId, newTerm) else d.persistentState.votedFor); currentTerm = newTerm}}
            (newState, newData)
    member x.HandleAppendEntries(s: state, d: data, r: AppendEntries): (state * data) =
        if r.leaderId = id then
            (s, d)
        else
            let logOk = r.prevLogIndex = 0 ||
                        r.prevLogIndex > 0 && r.prevLogIndex <= List.length d.persistentState.log && r.prevLogTerm = d.persistentState.log.[r.prevLogIndex - 1].term
            let result = r.term >= d.persistentState.currentTerm && logOk
            let (commitIndex, entries) =
                if result then
                    let index = r.prevLogIndex + 1
                    if List.isEmpty r.entries || List.length d.persistentState.log >= index && d.persistentState.log.[index - 1].term = r.entries[0].term then (r.leaderCommit, []) else 0
                else (d.volatileState.commitIndex, d.persistentState.log)
            //todo 2, 3, 4
            let newCommitIndex = if r.leaderCommit > d.volatileState.commitIndex then min r.leaderCommit 0 else d.volatileState.commitIndex//todo index of last entry
            let newTerm = max r.term d.persistentState.currentTerm
            x.Sender <! {term = newTerm; success = result}
            let newState = if r.term >= d.persistentState.currentTerm then Follower else s
            let newData = {d with persistentState = { d.persistentState with currentTerm = newTerm}; volatileState = {d.volatileState with commitIndex = newCommitIndex}}
            (newState, newData)
    member x.EmitAppendEntries(d: data, heartbeat: bool) =
        let parentPath = x.Self.Path.Parent
        let context = ActorBase.Context
        d.leaderState.Value.nextIndex |> Map.map (fun k v ->
            let path = parentPath.Child(sprintf "server%d" k)
            let tango = context.ActorSelection(path)
            let prevLogIndex = v - 1
            let prevLogTerm = if prevLogIndex > 0 then d.persistentState.log.[prevLogIndex - 1].term else 0
            let lastEntry = min (List.length d.persistentState.log) v
            let entriesToSend = d.persistentState.log.[v-1..lastEntry - 1]
            tango <! {
                leaderId = id
                term = d.persistentState.currentTerm
                leaderCommit = min lastEntry d.volatileState.commitIndex
                entries = entriesToSend
                prevLogIndex = prevLogIndex
                prevLogTerm = prevLogTerm
            }
        ) |> ignore

        
[<EntryPoint>]
let main argv = 
    use system = System.create "my-system" (Configuration.load())
    let rand = Random(1234)
    let servers = 2
    let rafters =
        [1..servers]
        |> List.map(fun id ->
            let properties = [| id :> obj; rand.Next(1, 3) :> obj; system.ActorSelection("/user/*") :> obj; servers :> obj |]
            system.ActorOf(Props(typedefof<Rafter>, properties), sprintf "server%d" id))
    rafters |> List.map (fun r -> printfn "%A" r.Path) |> ignore
    [1..10]
    |> List.map(fun id ->
        let a = async {
            do! Async.Sleep(500)
            rafters.Head <! id
        }
        Async.RunSynchronously(a)
    ) |> ignore

    
//    let handleMessage (mailbox: Actor<'a>) msg =
//        printf "msg\n"
//        mailbox.Sender() <! "dupa"
////        match msg with
////        | Some x -> printf "%A\n" x
////        | None -> mailbox.Sender <! "dupa"
//
//    let aref = spawn system "my-actor" (actorOf2 handleMessage)
////    let rec go (i: int) =
////        printf "sending\n"
////        aref <! (Some "aasd")
////        if i > 0 then
////            go(i - 1)
////    go(5)
//
////    let echoServer = 
////        spawn system "EchoServer"
////        <| fun mailbox ->
////            let rec loop() =
////                actor {
////                    let! message = mailbox.Receive()
////                    match box message with
////                    | :? string as msg ->
////                        mailbox.Sender() <! "Hello"
////                        return! loop()
////                    | _ ->  failwith "unknown message"
////                }
////            loop()
//
////    echoServer <! "F#!"
//    let echoServer = system.ActorOf(Props(typedefof<MyActor>, Array.empty))
//    let a = async {
//        do! Async.Sleep(100)
//        echoServer <! "F#!"
//        do! Async.Sleep(5000)
//        //printf "%A" response
//        //let! response1 = echoServer <? "F#!"
//        //printf "%A" response1
//    }
//    Async.RunSynchronously(a)
//
//    let testLoop = async {
//        for i in [1..100] do
//            // do something
//            printf "%i before.." i
//        
//            // sleep a bit 
//            do! Async.Sleep 10  
//            printfn "..after"
//    }
//    let cancellationSource = new CancellationTokenSource()
//
//    Async.Start (testLoop,cancellationSource.Token)
//
//    // wait a bit
//    Thread.Sleep(200)  
//    
//    // cancel after 200ms
//    cancellationSource.Cancel()
//    printfn "a"
    Console.ReadLine() |> ignore
    0 // return an integer exit code
