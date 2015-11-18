Imports System.Threading
Imports CrystalDecisions.CrystalReports.Engine
Imports CrystalDecisions.Shared
Imports System.IO
Imports System.Web.Mail
Imports System.Runtime.InteropServices
Imports System.Text.RegularExpressions
Imports System.Xml
Imports System.Configuration.ConfigurationSettings

Public Class EventThread
    Const evtMsg As String = "Processing Event: sub {0} {1} on {2} for {3} on thread {4} at {5}"
    Public Errors As Boolean

#Region " members "
    Private m_DataProvider As New DataProvider
    Private m_DataSource As DataSet
    Private m_Start As Integer
    Private m_Max As Integer
    Private m_RunDate As DateTime
    Private m_Thread As Thread
    Private m_Verbose As Boolean = False
    Private m_ThreadIndex As Integer
    Private m_Error As String = String.Empty
    Private m_Templates As String = String.Empty
    Private m_Export As String = String.Empty
    Private m_Subscription_Id As String = ""
#End Region

#Region " Properties "
    Public ReadOnly Property [Error]() As String
        Get
            Return m_Error
        End Get
    End Property

    Public Property Subscription_Id() As String
        Get
            Return m_Subscription_Id
        End Get
        Set(ByVal Value As String)
            m_Subscription_Id = Value
        End Set
    End Property

    Public Property ThreadIndex() As Integer
        Get
            Return m_ThreadIndex
        End Get
        Set(ByVal Value As Integer)
            m_ThreadIndex = Value
        End Set
    End Property

    Public ReadOnly Property Data() As DataProvider
        Get
            Return m_DataProvider
        End Get
    End Property

    Public Property ConnectionString() As String
        Get
            Return Me.Data.ConnectionString
        End Get
        Set(ByVal Value As String)
            Me.Data.ConnectionString = Value
        End Set
    End Property

    Public Property Warehouse() As String
        Get
            Return Me.Data.Warehouse
        End Get
        Set(ByVal Value As String)
            Me.Data.Warehouse = Value
        End Set
    End Property

    Public Property DataSource() As DataSet
        Get
            Return m_DataSource
        End Get
        Set(ByVal Value As DataSet)
            m_DataSource = Value
        End Set
    End Property

    Public Property StartRowHandle() As Integer
        Get
            Return m_Start
        End Get
        Set(ByVal Value As Integer)
            m_Start = Value
        End Set
    End Property

    Public Property MaxRows() As Integer
        Get
            Return m_Max
        End Get
        Set(ByVal Value As Integer)
            m_Max = Value
        End Set
    End Property

    Public Property RunDate() As DateTime
        Get
            Return m_RunDate
        End Get
        Set(ByVal Value As DateTime)
            m_RunDate = Value
        End Set
    End Property

    Public ReadOnly Property IsAlive() As Boolean
        Get
            If Not m_Thread Is Nothing Then
                Return m_Thread.IsAlive
            Else
                Return False
            End If
        End Get
    End Property

    Public ReadOnly Property IsBackgroud() As Boolean
        Get
            If Not m_Thread Is Nothing Then
                Return m_Thread.IsBackground
            Else
                Return False
            End If
        End Get
    End Property

    Public ReadOnly Property ThreadState() As ThreadState
        Get
            If Not m_Thread Is Nothing Then
                Return m_Thread.ThreadState
            Else
                Return ThreadState.Unstarted
            End If
        End Get
    End Property

    Public Property Verbose() As Boolean
        Get
            Return m_Verbose
        End Get
        Set(ByVal Value As Boolean)
            m_Verbose = Value
        End Set
    End Property

    Public Property TemplateDirectory() As String
        Get
            Return m_Templates
        End Get
        Set(ByVal Value As String)
            m_Templates = Value
        End Set
    End Property

    Public Property ExportDirectory() As String
        Get
            Return m_Export
        End Get
        Set(ByVal Value As String)
            m_Export = Value
        End Set
    End Property
#End Region

    Public Sub New()
    End Sub

    Public Sub New(ByVal ConnectionString As String, ByVal Warehouse As String)
        Me.ConnectionString = ConnectionString
        Me.Warehouse = Warehouse
    End Sub

    Public Sub Start()
        If m_Subscription_Id = "" Then
            m_Thread = New Thread(AddressOf Me.StartThread)
        Else
            m_Thread = New Thread(AddressOf Me.SubscriptionThread)
        End If
        m_Thread.Start()
    End Sub

    Private Sub StartThread()
        Try
            Me.DataSource = Me.Data.GetEvents(Me.RunDate, Me.StartRowHandle, Me.MaxRows)
            For Each dr As DataRow In Me.DataSource.Tables("Fulfillment_Subscriptions").Rows
                Try
                    Me.ProcessEvent(dr)
                Catch ex As Exception
                    m_Error += IIf(m_Error = "", "", vbCrLf) + ex.Message
                End Try
            Next
        Catch ex As Exception
            m_Error = ex.Message
        End Try
    End Sub

    Public Sub SubscriptionThread()
        Try
            Me.DataSource = Me.Data.GetEventsById(m_Subscription_Id)
            For Each dr As DataRow In Me.DataSource.Tables("Fulfillment_Subscriptions").Rows
                Try
                    Me.ProcessEvent(dr)
                Catch ex As Exception
                    m_Error += IIf(m_Error = "", "", vbCrLf) + ex.Message
                End Try
            Next
        Catch ex As Exception
            m_Error = ex.Message
        End Try
    End Sub

    Public Sub ProcessEvent(ByVal sr As DataRow)
        Dim eds As DataSet
        Dim evtTable As DataTable
        Dim evtView As DataView
        Dim nextRun As DateTime
        Dim hours As Integer = 0
        Dim fileName As String = String.Empty
        Dim key_field As String
        Dim email_field As String
        Dim protime As TimeSpan
        Dim starttime As DateTime
        Dim endtime As DateTime

        starttime = Now

        If Me.Verbose Then
            Console.WriteLine(String.Format(evtMsg, sr.Item("subscription_id"), sr.Item("event_name"), sr.Item("fulfillment_id"), sr.Item("full_name"), Me.ThreadIndex, DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")))
        End If
        Errors = False
        key_field = CStr(sr.Item("key_field"))
        email_field = CStr(sr.Item("email_field"))

        eds = Me.Data.GetEventData(sr.Item("event_view"), _
                sr.Item("default_filter"), _
                sr.Item("filter"), _
                sr.Item("fulfillment_id"), _
                sr.Item("last_run"), _
                Me.RunDate, key_field) 'RT added Key field to sort the data

        evtTable = eds.Tables(CStr(sr.Item("event_view")))
        evtView = evtTable.DefaultView

        If evtTable.Rows.Count > 0 Then
            If key_field = "" Then
                Try
                    fileName = Me.CreateDocument(sr, eds)
                    Me.SendEmail(sr, fileName, String.Empty, evtTable.Rows(0))
                Catch ex As Exception
                    Errors = True
                    If Me.Verbose Then
                        Console.WriteLine("Error: " + ex.Message)
                    End If
                End Try
            Else
                Dim edsClone As DataSet = eds.Clone()
                Dim evtClone As DataTable = edsClone.Tables(CStr(sr.Item("event_view")))
                Dim drv As DataRowView
                Dim key As Object
                Dim ea As String = String.Empty

                For i As Integer = 0 To evtView.Count - 1
                    drv = evtView.Item(i)
                    If Not key Is Nothing AndAlso key <> drv.Item(key_field) Then
                        Try
                            fileName = Me.CreateDocument(sr, edsClone)
                            Me.SendEmail(sr, fileName, ea, evtClone.Rows(0))
                        Catch ex As Exception
                            Errors = True
                            If Me.Verbose Then
                                Console.WriteLine("Error: " + ex.Message)
                            End If
                        End Try

                        Me.Data.LogEvent(sr.Item("fulfillment_id"), _
                                sr.Item("customer_id"), _
                                sr.Item("event_id"), _
                                fileName, ea)

                        ea = String.Empty
                        edsClone.Clear()
                    End If

                    If email_field <> "" AndAlso ea = "" Then
                        If IsDBNull(drv.Item(email_field)) Then
                            ea = ""
                        Else
                            ea = CStr(drv.Item(email_field))
                        End If
                    End If

                    evtClone.Rows.Add(drv.Row.ItemArray)

                    key = drv.Item(key_field)
                Next i

                If evtClone.Rows.Count > 0 Then
                    Try
                        fileName = Me.CreateDocument(sr, edsClone)
                        Me.SendEmail(sr, fileName, ea, evtClone.Rows(0))
                    Catch ex As Exception
                        Errors = True
                        If Me.Verbose Then
                            Console.WriteLine("Error: " + ex.Message)
                        End If
                    End Try

                    Me.Data.LogEvent(sr.Item("fulfillment_id"), _
                            sr.Item("customer_id"), _
                            sr.Item("event_id"), _
                            fileName, ea)
                End If
            End If
        End If


        Try
            Me.Data.LogEvent(sr.Item("fulfillment_id"), _
                    sr.Item("customer_id"), _
                    sr.Item("event_id"), _
                    fileName, "")

            nextRun = CDate(sr.Item("next_run"))
            hours = CInt(sr.Item("hours"))
            If hours < 1 Then
                hours = 1
            End If

            Do
                nextRun = nextRun.AddHours(hours)
            Loop Until nextRun > Me.RunDate
            'If Errors = False Then
            Me.Data.UpdateSubscription(sr.Item("subscription_id"), Me.RunDate, nextRun)
            'End If
        Catch ex As Exception
            If Me.Verbose Then
                Console.WriteLine("Error: " + ex.Message)
            End If
        End Try

        If Me.Verbose Then
            Console.WriteLine(sr.Item("event_view") + ": " + evtTable.Rows.Count.ToString)
        End If
        endtime = Now
        protime = starttime.Subtract(endtime)
        Console.WriteLine("The process finished in " + protime.Minutes.ToString + " " + protime.Seconds.ToString + " " + m_Error)

    End Sub

    Public Function CreateDocument(ByVal sr As DataRow, ByVal EventData As DataSet) As String
        Dim template As String = CStr(sr.Item("template"))
        Dim format As String = CStr(sr.Item("format"))
        Dim rptDoc As New ReportDocument
        Dim fileName As String
        Dim exportDir As String
        Dim eft As ExportFormatType
        Dim excelFormatOpts As New ExcelFormatOptions
        Dim htmlFormatOpts As New HTMLFormatOptions
        Dim diskOpts As New DiskFileDestinationOptions
        Dim eat As String = "" 'ExcelAreaType
        Dim eagn As String = "" 'ExcelAreaGroupNumber
        Dim ExportPath As String

        exportDir = Me.ExportDirectory + Now.ToString("yyyyMMdd") + "\"

        If Not Directory.Exists(exportDir) Then
            Directory.CreateDirectory(exportDir)
        End If

        Console.WriteLine("Loading report: " + Me.TemplateDirectory + template)

        Try
            rptDoc.Load(Me.TemplateDirectory + template, OpenReportMethod.OpenReportByDefault)
        Catch ex As Exception
            Throw New Exception("File load error: " + ex.Message)
        End Try


        Select Case format.ToLower
            Case "pdf"
                fileName = exportDir + Guid.NewGuid.ToString + ".pdf"
                eft = ExportFormatType.PortableDocFormat
            Case "excel"
                fileName = exportDir + Guid.NewGuid.ToString + ".xls"
                eat = Me.GetTemplateValue(template, "Excel", "ExcelAreaType")
                eagn = Me.GetTemplateValue(template, "Excel", "ExcelAreaGroupNumber")
                eft = ExportFormatType.Excel
                excelFormatOpts.ExcelUseConstantColumnWidth = False

                If eat <> "" Then
                    excelFormatOpts.ExcelAreaType = CInt(eat)
                End If
                If eagn <> "" Then
                    excelFormatOpts.ExcelAreaGroupNumber = CInt(eagn)
                End If

                rptDoc.ExportOptions.FormatOptions = excelFormatOpts

            Case "excel_data_only"
                fileName = exportDir + Guid.NewGuid.ToString + ".xls"
                eat = Me.GetTemplateValue(template, "Excel", "ExcelAreaType")
                eagn = Me.GetTemplateValue(template, "Excel", "ExcelAreaGroupNumber")
                eft = ExportFormatType.ExcelRecord
                excelFormatOpts.ExcelUseConstantColumnWidth = False

                If eat <> "" Then
                    excelFormatOpts.ExcelAreaType = CInt(eat)
                End If
                If eagn <> "" Then
                    excelFormatOpts.ExcelAreaGroupNumber = CInt(eagn)
                End If

                rptDoc.ExportOptions.FormatOptions = excelFormatOpts

            Case "html40"
                fileName = exportDir + Guid.NewGuid.ToString + ".htm"
                eft = ExportFormatType.HTML40
                htmlFormatOpts.HTMLFileName = fileName
                htmlFormatOpts.HTMLEnableSeparatedPages = False
                htmlFormatOpts.HTMLHasPageNavigator = False
                rptDoc.ExportOptions.FormatOptions = htmlFormatOpts
                diskOpts.DiskFileName = fileName
                rptDoc.ExportOptions.DestinationOptions = diskOpts
                rptDoc.ExportOptions.ExportDestinationType = ExportDestinationType.DiskFile
                rptDoc.ExportOptions.ExportFormatType = ExportFormatType.HTML40

            Case "html32"
                fileName = exportDir + Guid.NewGuid.ToString + ".htm"
                eft = ExportFormatType.HTML32
                htmlFormatOpts.HTMLFileName = fileName
                htmlFormatOpts.HTMLEnableSeparatedPages = False
                htmlFormatOpts.HTMLHasPageNavigator = False
                rptDoc.ExportOptions.FormatOptions = htmlFormatOpts
                diskOpts.DiskFileName = fileName
                rptDoc.ExportOptions.DestinationOptions = diskOpts
                rptDoc.ExportOptions.ExportDestinationType = ExportDestinationType.DiskFile
                rptDoc.ExportOptions.ExportFormatType = ExportFormatType.HTML32

            Case "richtext"
                fileName = exportDir + Guid.NewGuid.ToString + ".rtf"
                eft = ExportFormatType.RichText
            Case "wordforwindows"
                fileName = exportDir + Guid.NewGuid.ToString + ".doc"
                eft = ExportFormatType.WordForWindows
            Case Else
                fileName = exportDir + Guid.NewGuid.ToString + ".pdf"
                eft = ExportFormatType.PortableDocFormat
        End Select

        If Not IsDBNull(template) AndAlso template <> "" Then
            rptDoc.SetDataSource(EventData)

            Try
                rptDoc.ExportToDisk(eft, fileName)
            Catch ex As Exception
                Throw New Exception("File export error: " + ex.Message)
            End Try
            '?????????????????
            'If Left(format.ToLower, 4) = "html" Then
            '    Try
            '        File.Move(CurDir() + "\" + Left(Trim(template), Len(template) - 4) + "\" + fileName, exportDir + fileName)
            '        fileName = exportDir + fileName
            '    Catch ex As Exception
            '        Throw New Exception("File move error: " + ex.Message)
            '    End Try
            'End If
        End If

        rptDoc.Close()

        Return fileName
    End Function

    Public Sub SendEmail(ByVal sr As DataRow, ByVal fileName As String, ByVal email As String, ByVal firstRow As DataRow)
        Dim msg As New MailMessage
        Dim mat As MailAttachment
        Dim from As String = ""
        Dim subject As String = ""
        Dim body As String = ""
        Dim sendto As String = ""
        Dim bcc As String = ""
        Dim reMatches As MatchCollection
        Dim field As String
        Dim ie As Exception
        Dim nFile As Integer
        Dim htmlData As String

        If File.Exists(fileName) Then
            mat = New MailAttachment(fileName)
        End If

        from = CStr(sr.Item("efrom"))
        subject = CStr(sr.Item("subject"))
        body = CStr(sr.Item("ebody"))
        sendto = CStr(sr.Item("email"))

        If IsDBNull(sr.Item("bcc")) Then
            bcc = ""
        Else
            bcc = CStr(sr.Item("bcc"))
        End If

        If sendto = "" Then
            sendto = email
        Else
            If email <> "" Then
                sendto += "; " + email
            End If
        End If

        sendto = sendto.Trim

        If sendto.StartsWith(";") Then
            sendto = sendto.Substring(1)
        End If

        If from = "" Then
            from = "ops@aerofulfillment.com"
        End If

        If subject = "" Then
            subject = CStr(sr.Item("esubject"))
        End If

        If subject = "" Then
            subject = "AeroNavigator Information Request"
        End If

        reMatches = Regex.Matches(body, "\{[0-9a-zA-Z\._@]{0,}\}")

        For Each m As Match In reMatches
            field = m.Value.Substring(1, m.Value.Length - 2)
            If sr.Table.Columns.Contains(field) Then
                If IsDBNull(sr.Item(field)) Then
                    body = body.Replace(m.Value, "")


                Else
                    body = body.Replace(m.Value, CStr(sr.Item(field)))
                End If
            End If
        Next

        If Not firstRow Is Nothing Then
            reMatches = Regex.Matches(subject, "\{[0-9a-zA-Z\._@]{0,}\}")
            For Each m As Match In reMatches
                field = m.Value.Substring(1, m.Value.Length - 2)
                If firstRow.Table.Columns.Contains(field) Then
                    If IsDBNull(firstRow.Item(field)) Then
                        subject = subject.Replace(m.Value, "")
                    Else
                        subject = subject.Replace(m.Value, CStr(firstRow.Item(field)))
                    End If
                End If
            Next
        End If

        With msg
            .Priority = MailPriority.Normal ' m.hancock - 02/09/2012 01:19:36 PM - CMR 20120201.016. Removed High Priority setting.
            .From = from
            .Subject = subject
            .To = sendto
            .Bcc = bcc
            If Right(fileName, 3) = "htm" Or Right(fileName, 4) = "html" Then
                htmlData = FileToString(fileName)
                .BodyFormat = MailFormat.Html
                .Body = htmlData
                mat = Nothing
            Else
                .BodyFormat = MailFormat.Text
                .Body = body
            End If
            If Not mat Is Nothing Then
                .Attachments.Add(mat)
            End If
        End With

        SmtpMail.SmtpServer = My.Settings.SMTPSetting

        Dim wh As String = My.Settings.Warehouse
        Dim cs As String = My.Settings.ConnectionString
        Dim dp As New DataProvider(wh, cs)
        dp.LogMail(0, 0, sendto, from, subject, "", "Sending Mail", fileName, 0)
        Try
            SmtpMail.Send(msg)
            Console.WriteLine("Email sent to " + sendto + " at " + DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"))
        Catch ex As Exception
            Errors = True
            Console.WriteLine("SMTP Error: From - " + from)
            Console.WriteLine("SMTP Error: To - " + sendto)
            Console.WriteLine("SMTP Error: " + ex.Message)
            dp.LogMail(0, 0, sendto, from, subject, ex.Message, "Error", fileName, 0)
            ie = ex.InnerException
            Do While Not ie Is Nothing
                Console.WriteLine("SMTP Error: " + ie.Message)
                dp.LogMail(0, 0, sendto, from, subject, ie.Message, "Error", fileName, 0)
                ie = ie.InnerException
            Loop
        End Try
    End Sub

    Public Function GetTemplateValue(ByVal template As String, ByVal section As String, ByVal value As String) As String
        Dim path As String = Me.TemplateDirectory

        If Not File.Exists(path + template + ".xml") Then
            Return ""
        End If

        Dim xdoc As New XmlDocument
        Dim xsection As XmlNode
        Dim xvalue As XmlNode

        xdoc.Load(path + template + ".xml")

        xsection = xdoc.SelectSingleNode("//" + section + "/" + value)

        If xsection Is Nothing Then
            Return ""
        Else
            Return xsection.InnerText
        End If
    End Function

    Public Function FileToString(ByVal fileName As String) As String
        Dim lcResult As String = ""

        Try
            Dim loStream As New StreamReader(fileName)
            lcResult = loStream.ReadToEnd()
            loStream.Close()
        Catch ex As Exception
            lcResult = ""
        End Try

        Return lcResult
    End Function

#Region " thread methods "
    Public Sub Abort()
        If Not m_Thread Is Nothing Then
            m_Thread.Abort()
        End If
    End Sub

    Public Sub ResetAbort()
        If Not m_Thread Is Nothing Then
            m_Thread.ResetAbort()
        End If
    End Sub

    Public Sub [Resume]()
        If Not m_Thread Is Nothing Then
            m_Thread.Resume()
        End If
    End Sub

    Public Sub Sleep(ByVal millisecondsTimeout As Integer)
        If Not m_Thread Is Nothing Then
            m_Thread.Sleep(millisecondsTimeout)
        End If
    End Sub

    Public Sub Sleep(ByVal Timeout As TimeSpan)
        If Not m_Thread Is Nothing Then
            m_Thread.Sleep(Timeout)
        End If
    End Sub

    Public Sub SpinWait(ByVal iterations As Integer)
        If Not m_Thread Is Nothing Then
            m_Thread.SpinWait(iterations)
        End If
    End Sub

    Public Sub Suspend()
        If Not m_Thread Is Nothing Then
            m_Thread.Suspend()
        End If
    End Sub

    Public Sub Join()
        If Not m_Thread Is Nothing Then
            m_Thread.Join()
        End If
    End Sub
#End Region
End Class
