Imports System.Data
Imports System.Data.Common
Imports System.Data.SqlClient
Imports System.Text

Public Class DataProvider

#Region " members "
    Private m_Warehouse As String
    Private m_ConnectionString As String
    Private m_SqlConnection As New SqlConnection
    Private m_SqlDataAdapter As New SqlDataAdapter
#End Region

#Region " properties "
    Public Property Warehouse() As String
        Get
            Return m_Warehouse
        End Get
        Set(ByVal Value As String)
            m_Warehouse = Value
        End Set
    End Property

    Public Property ConnectionString() As String
        Get
            Return m_ConnectionString
        End Get
        Set(ByVal Value As String)
            m_ConnectionString = Value
            m_SqlConnection.ConnectionString = m_ConnectionString
        End Set
    End Property
#End Region

    Public Sub New()
    End Sub

    Public Sub New(ByVal Warehouse As String, ByVal ConnectionString As String)
        Me.Warehouse = Warehouse
        Me.ConnectionString = ConnectionString
    End Sub

    Public Function GetEvents(ByVal RunDate As DateTime, ByVal StartRowHandle As Integer, ByVal MaxRows As Integer) As DataSet
        Dim cmd As New SqlCommand
        Dim ds As New DataSet("Events")
        Dim sqlText As New StringBuilder

        sqlText.Append("SELECT * FROM Subscription_Events")
        sqlText.Append(" WHERE whse_id = @warehouse")
        sqlText.Append(" AND status = @status AND next_run <= @rundate")
        sqlText.Append(" ORDER BY next_run;")

        With cmd
            .CommandText = sqlText.ToString
            .CommandType = CommandType.Text
            .Connection = m_SqlConnection
            .Parameters.AddWithValue("@warehouse", Warehouse)
            .Parameters.AddWithValue("@status", "ACTIVE")
            .Parameters.AddWithValue("@rundate", RunDate)
        End With

        With m_SqlDataAdapter
            .TableMappings.Clear()
            .TableMappings.AddRange(New DataTableMapping() {New DataTableMapping("Table", "Fulfillment_Subscriptions")})
            .SelectCommand = cmd
        End With

        Try
            m_SqlDataAdapter.Fill(ds, StartRowHandle, MaxRows, "Table")
            Return ds
        Catch ex As Exception
            Throw New Exception(ex.Message, ex)
        End Try

        Return Nothing
    End Function

    Public Function GetEventCount(ByVal RunDate As DateTime) As Integer
        Dim cmd As New SqlCommand
        Dim sqlText As New StringBuilder
        Dim count As Integer = 0

        sqlText.Append("SELECT COUNT(*) AS count FROM Fulfillment_Subscriptions fs(nolock)")
        sqlText.Append(" INNER JOIN Fulfillment f(nolock) ON fs.fulfillment_id = f.fulfillment_id")
        sqlText.Append(" WHERE f.whse_id = @warehouse AND fs.next_run <= @rundate;")

        With cmd
            .CommandType = CommandType.Text
            .CommandText = sqlText.ToString
            .Parameters.AddWithValue("@warehouse", Warehouse)
            .Parameters.AddWithValue("@rundate", RunDate)
            .Connection = m_SqlConnection
        End With

        Try
            If m_SqlConnection.State = ConnectionState.Closed Then
                m_SqlConnection.Open()
            End If
            count = cmd.ExecuteScalar()
        Catch ex As Exception
            Console.WriteLine(ex.Message)
        Finally
            If m_SqlConnection.State = ConnectionState.Open Then
                m_SqlConnection.Close()
            End If
        End Try

        Return count
    End Function

    Public Function GetEventData(ByVal viewName As String, ByVal default_filter As String, ByVal filter As String, ByVal fulfillment_id As Integer, ByVal lastRun As DateTime, ByVal runDate As DateTime, keyfield As String) As DataSet
        Dim cmd As New SqlCommand
        Dim ds As New DataSet(viewName)
        Dim sqlText As New StringBuilder

        sqlText.Append("SELECT * FROM " + viewName + " with (nolock)")
        If Not default_filter.Equals(String.Empty) Then
            sqlText.Append(" WHERE " + default_filter)
        End If

        If Not filter.Equals(String.Empty) Then
            If sqlText.ToString.IndexOf("WHERE") > -1 Then
                sqlText.Append(" AND " + filter)
            Else
                sqlText.Append(" WHERE " + filter)
            End If
        End If

        'Add key field sorting if needed 
        If keyfield <> String.Empty Then
            sqlText.Append(" ORDER BY ")
            sqlText.Append(keyfield)
        End If

        With cmd
            .CommandText = sqlText.ToString
            .CommandType = CommandType.Text
            .CommandTimeout = 600
            .Connection = m_SqlConnection
            .Parameters.AddWithValue("@fulfillment_id", fulfillment_id)
            .Parameters.AddWithValue("@last_run", lastRun)
            .Parameters.AddWithValue("@run_date", runDate)
        End With

        With m_SqlDataAdapter
            .TableMappings.Clear()
            .TableMappings.AddRange(New DataTableMapping() {New DataTableMapping("Table", viewName)})
            .SelectCommand = cmd
        End With

        Try
            m_SqlDataAdapter.Fill(ds)
            Return ds
        Catch ex As Exception
            Throw New Exception(ex.Message, ex)
        End Try

        Return Nothing
    End Function

    Public Sub LogEvent(ByVal fulfillment_id As Integer, ByVal customer_id As Integer, ByVal event_id As Integer, ByVal file_name As String, ByVal email As String)
        Dim cmd As New SqlCommand
        Dim sqlText As New StringBuilder

        sqlText.Append("INSERT INTO Fulfillment_Event_Log(fulfillment_id,customer_id,event_id,file_name,add_date,edit_date,email)")
        sqlText.Append(" VALUES (@fulfillment_id,@customer_id,@event_id,@file_name,GetDate(),GetDate(),@email)")

        With cmd
            .CommandText = sqlText.ToString
            .CommandType = CommandType.Text
            .Connection = m_SqlConnection
            .Parameters.AddWithValue("@fulfillment_id", fulfillment_id)
            .Parameters.AddWithValue("@customer_id", customer_id)
            .Parameters.AddWithValue("@event_id", event_id)
            .Parameters.AddWithValue("@file_name", file_name)
            .Parameters.AddWithValue("@email", email)
        End With

        Try
            If m_SqlConnection.State = ConnectionState.Closed Then
                m_SqlConnection.Open()
            End If
            cmd.ExecuteNonQuery()
        Catch ex As Exception
            Throw New Exception(ex.Message, ex)
        Finally
            If m_SqlConnection.State = ConnectionState.Open Then
                m_SqlConnection.Close()
            End If
        End Try
    End Sub
    Public Sub LogMail(ByVal fulfillment_id As Integer, ByVal customer_id As Integer, ByVal to_address As String, ByVal from_address As String, ByVal subject As String, ByVal err As String, ByVal message As String, ByVal file_name As String, ByVal subscription_id As String)

        Dim cmd As New SqlCommand
        Dim sqlText As New StringBuilder

        sqlText.Append("INSERT INTO Mail_Log(fulfillment_id,customer_id,to_address,from_address,subject,err,message,file_name,subscription_id,adddate)")
        sqlText.Append(" VALUES (@fulfillment_id,@customer_id,@to_address,@from_address,@subject,@err,@message,@file_name,@subscription_id,GetDate())")

        With cmd
            .CommandText = sqlText.ToString
            .CommandType = CommandType.Text
            .Connection = m_SqlConnection
            .Parameters.AddWithValue("@fulfillment_id", fulfillment_id)
            .Parameters.AddWithValue("@customer_id", customer_id)
            .Parameters.AddWithValue("@to_address", to_address)
            .Parameters.AddWithValue("@from_address", from_address)
            .Parameters.AddWithValue("@subject", subject)
            .Parameters.AddWithValue("@err", err)
            .Parameters.AddWithValue("@message", message)
            .Parameters.AddWithValue("@file_name", file_name)
            .Parameters.AddWithValue("@subscription_id", subscription_id)
        End With

        Try
            If m_SqlConnection.State = ConnectionState.Closed Then
                m_SqlConnection.Open()
            End If
            cmd.ExecuteNonQuery()
        Catch ex As Exception
            Throw New Exception(ex.Message, ex)
        Finally
            If m_SqlConnection.State = ConnectionState.Open Then
                m_SqlConnection.Close()
            End If
        End Try
    End Sub

    Public Sub UpdateSubscription(ByVal subscription_id As String, ByVal last_run As DateTime, ByVal next_run As DateTime)
        Dim cmd As New SqlCommand
        Dim sqlText As New StringBuilder

        sqlText.Append("UPDATE Fulfillment_Subscriptions SET last_run = @last_run, next_run = @next_run")
        sqlText.Append(" WHERE subscription_id = @subscription_id")

        With cmd
            .CommandText = sqlText.ToString
            .CommandType = CommandType.Text
            .Connection = m_SqlConnection
            .Parameters.AddWithValue("@subscription_id", subscription_id)
            .Parameters.AddWithValue("@last_run", last_run)
            .Parameters.AddWithValue("@next_run", next_run)
        End With

        Try
            If m_SqlConnection.State = ConnectionState.Closed Then
                m_SqlConnection.Open()
            End If
            cmd.ExecuteNonQuery()
        Catch ex As Exception
            Throw New Exception(ex.Message, ex)
        Finally
            If m_SqlConnection.State = ConnectionState.Open Then
                m_SqlConnection.Close()
            End If
        End Try
    End Sub

    Public Function GetEventsById(ByVal subscription_id As String) As DataSet
        Dim cmd As New SqlCommand
        Dim ds As New DataSet("Events")
        Dim sqlText As New StringBuilder

        sqlText.Append("SELECT * FROM Subscription_Events")
        sqlText.Append(" WHERE whse_id = @warehouse")
        sqlText.Append(" AND subscription_id = @subscription_id")
        sqlText.Append(" ORDER BY next_run;")

        With cmd
            .CommandText = sqlText.ToString
            .CommandType = CommandType.Text
            .Connection = m_SqlConnection
            .Parameters.AddWithValue("@warehouse", Warehouse)
            .Parameters.AddWithValue("@subscription_id", subscription_id)
        End With

        With m_SqlDataAdapter
            .TableMappings.Clear()
            .TableMappings.AddRange(New DataTableMapping() {New DataTableMapping("Table", "Fulfillment_Subscriptions")})
            .SelectCommand = cmd
        End With

        Try
            m_SqlDataAdapter.Fill(ds)
            Return ds
        Catch ex As Exception
            Throw New Exception(ex.Message, ex)
        End Try

        Return Nothing
    End Function
End Class
