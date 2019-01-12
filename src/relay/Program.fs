namespace Relay

open Hopac
open System.IO.Pipelines
open System.Net.Sockets
open System
open System.Buffers

module Multiplexing =
    let hello name =
        run <| Job.fromAsync (async { printfn "Hello" })
        printfn "Hello %s" name
    
    let fillPipeFromSocket (socket:Socket) (writer:PipeWriter) =
        let minBufferSize = 512
        let rec iter () =
            job {
                let memory = writer.GetMemory minBufferSize
                try
                    let! bytesRead = (fun () -> socket.ReceiveAsync(memory, SocketFlags.None).AsTask() ) |> Job.fromTask
                    if bytesRead <> 0 then
                        writer.Advance bytesRead
                        let! flushResult = (fun () -> writer.FlushAsync().AsTask()) |> Job.fromTask
                        if not flushResult.IsCompleted then
                            do! iter ()
                with
                | ex -> eprintfn "Error processing socket: %O" ex
            }
        job {
            do! iter ()
            writer.Complete ()
        }

    let rec readPipe (reader:PipeReader) (processBuffer:ReadOnlySequence<byte> -> Job<unit>) =
        job {
            let! result = (fun () -> reader.ReadAsync().AsTask()) |> Job.fromTask
            let buffer = result.Buffer
            do! processBuffer buffer
            reader.AdvanceTo (buffer.Start, buffer.End)
            if not result.IsCompleted then
                do! readPipe reader processBuffer
            else
                reader.Complete ()
        }
        
    let fillSocketFromPipe (socket:Socket) (memory:ReadOnlySequence<byte>) =
        job {
            let mutable en = memory.GetEnumerator()
            let rec loop () =
                job {
                    let! _ = (fun () -> socket.SendAsync(en.Current, SocketFlags.None).AsTask() ) |> Job.fromTask
                    if en.MoveNext () then
                        do! loop ()
                }
            do! loop ()
        }

    let proxyStreams (source:System.IO.Stream) (destination:System.IO.Stream) =
        let (buffer:byte array) = Array.zeroCreate 4096
        let rec loop () =
            job {
                let! bytesRead = (fun () -> source.ReadAsync (buffer, 0, buffer.Length)) |> Job.fromTask
                if bytesRead = 0 then
                    // Nothing read from source means the stream was closed.
                    destination.Close ()
                else
                    do! (fun () -> destination.WriteAsync (buffer, 0, bytesRead)) |> Job.fromUnitTask
                    do! (fun () -> destination.FlushAsync ()) |> Job.fromUnitTask
                    do! loop ()
            }
        loop ()

    let proxySocketWithPipe (source:System.Net.Sockets.Socket) (destination:System.Net.Sockets.Socket) =
        job {
            let pipe = Pipe ()
            do! fillPipeFromSocket source pipe.Writer
            do! readPipe pipe.Reader (fillSocketFromPipe destination)
        }

module Program =
    open Hopac.Infixes
    open System.Net
    
    [<EntryPoint>]    
    let main argv =
        let listenPort = 3456
        let listener = TcpListener(IPAddress.Any, listenPort)
        //listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, false)
        listener.Start ()
        Console.WriteLine("Listening on {0}", listenPort)
        while true do
            let client = listener.AcceptTcpClient ()
            Console.WriteLine "Got client."
            
            use target = new TcpClient("127.0.0.1", 8899)
            let sourceSocket = client.Client
            let targetSocket = target.Client
            
            let clientToTarget = Multiplexing.proxyStreams (client.GetStream()) (target.GetStream())
            let targetToClient = Multiplexing.proxyStreams (target.GetStream()) (client.GetStream())
            
            try
                clientToTarget <*> targetToClient |> run |> ignore
            with
            | ex -> printfn "Connection ended: %A" ex
    
        //Multiplexing.proxySocketWithPipe sourceSocket targetSocket |> queueIgnore
        //Multiplexing.proxySocketWithPipe targetSocket sourceSocket |> queueIgnore
        Console.ReadLine () |> ignore
        0