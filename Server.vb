Imports System.Configuration.ConfigurationSettings
Imports System.IO

Module Server
    Public Sub Main()
        Dim subscription_id As String = Command()

        If subscription_id = "" Then
            System.Environment.ExitCode = StartServer()
        Else
            System.Environment.ExitCode = ExecuteSubscription(subscription_id)
        End If
    End Sub

    Public Function StartServer() As Integer
        Dim exitCode As Integer = 0
        Dim sw As StreamWriter
        Dim wh As String = My.Settings.Warehouse
        Dim cs As String = My.Settings.ConnectionString
        Dim tc As Integer = CInt(My.Settings.ThreadCount)
        Dim td As String = My.Settings.TemplateDirectory
        Dim ed As String = My.Settings.ExportDirectory

        Dim verbose As Boolean = CBool(My.Settings.Verbose)
        Dim cnsl As Boolean = CBool(My.Settings.Console)

        Console.WriteLine("Event Server: Started at " + Now.ToString)

        If Not cnsl Then
            sw = New StreamWriter("EventServer.log", True)
            sw.AutoFlush = True
            Console.SetOut(sw)
            Console.WriteLine("Event Server: Started at " + Now.ToString)
        End If

        Dim dp As New DataProvider(wh, cs)
        dp.LogMail(0, 0, "", "", "", "", "Event Server Started", "", 0)
        Dim RunDate As DateTime = Now
        Dim thdPool(tc - 1) As EventThread

        Dim events As Integer = 0
        Dim perThread As Integer = 1

        events = dp.GetEventCount(RunDate)

        If events <= 0 Then Exit Function

        perThread = CInt(Math.Ceiling(events / tc))

        For i As Integer = 0 To (tc - 1)
            If events > 0 Then
                If verbose Then
                    Console.WriteLine(String.Format("Thread {0} StartRow: {1} Max Rows: {2}", i, (i * perThread), perThread))
                End If
                thdPool(i) = New EventThread(cs, wh)
                With thdPool(i)
                    .ThreadIndex = i
                    .RunDate = RunDate
                    .StartRowHandle = i * perThread
                    .MaxRows = perThread
                    .Verbose = verbose
                    .TemplateDirectory = td
                    .ExportDirectory = ed
                    .Start()
                End With
            End If
            events -= perThread
        Next

        For i As Integer = 0 To (tc - 1)
            If Not thdPool(i) Is Nothing Then
                thdPool(i).Join()
                If thdPool(i).Error <> String.Empty Then
                    exitCode = -1
                    Console.WriteLine("Server Error: " + thdPool(i).Error)
                    dp.LogMail(0, 0, "", "", "", thdPool(i).Error, "Error:" & thdPool(i).Error, "", 0)
                End If
            End If
        Next

        Console.WriteLine("Event Server: Exiting with code " + exitCode.ToString + " at " + Now.ToString)
        dp.LogMail(0, 0, "", "", "", "", "Event Server Ended", "", 0)

        Return exitCode
    End Function

    Public Function ExecuteSubscription(ByVal subscription_id As String) As Integer
        Dim exitCode As Integer = 0
        Dim sw As StreamWriter
        Dim wh As String = My.Settings.Warehouse
        Dim cs As String = My.Settings.ConnectionString
        Dim tc As Integer = CInt(My.Settings.ThreadCount)
        Dim td As String = My.Settings.TemplateDirectory
        Dim ed As String = My.Settings.ExportDirectory

        Dim verbose As Boolean = CBool(My.Settings.Verbose)
        Dim cnsl As Boolean = CBool(My.Settings.Console)

        Console.WriteLine("Event Server: Started at " + Now.ToString)

        If Not cnsl Then
            sw = New StreamWriter("EventServer.log", True)
            sw.AutoFlush = True
            Console.SetOut(sw)
            Console.WriteLine("Event Server: Started at " + Now.ToString)
        End If

        Dim dp As New DataProvider(wh, cs)
        'dp.LogMail(0, 0, "", "", "", "", "Execute SubscriptionID:" & Trim(Str(subscription_id)), "", subscription_id)
        Dim et As EventThread

        et = New EventThread(cs, wh)
        With et
            .ThreadIndex = 0
            .RunDate = Now
            .StartRowHandle = 0
            .MaxRows = 0
            .Verbose = verbose
            .TemplateDirectory = td
            .ExportDirectory = ed
            .Subscription_Id = subscription_id
            .SubscriptionThread()
        End With

        Return exitCode
    End Function
End Module
