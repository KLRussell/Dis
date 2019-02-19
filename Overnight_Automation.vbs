const My_SQL_Server = ""
const My_SQL_DB = ""
const DOH_TBL = ""

Public WshShell, oFSO, Log_Path, networkInfo, SQL_Filepath, SQL_Filepath2

Set WshShell = CreateObject("WScript.Shell")
Set oFSO = CreateObject("Scripting.FileSystemObject")
Set networkInfo = CreateObject("WScript.NetWork")

Log_Path = oFSO.GetParentFolderName(WScript.ScriptFullName) & "\Logs\" & left(oFSO.GetFileName(WScript.ScriptFullName), len(oFSO.GetFileName(WScript.ScriptFullName)) - 4) & "_Log.txt"

SQL_Filepath = oFSO.GetParentFolderName(WScript.ScriptFullName) & "\SQL\Dispute_Fact_Upload.sql"
SQL_Filepath2 = oFSO.GetParentFolderName(WScript.ScriptFullName) & "\SQL\CNR_MRC.sql"

Execute_SQL SQL_Filepath
Execute_SQL SQL_Filepath2

Execute_SQL "exec SbFinDispute.TAT_CNRUpload_CU"

write_log Now() & " * Successfully uploaded Dispute_Fact_Upload"

Sub Append_ODS(myquery)
	On Error Resume Next
	Dim constr, conn

	constr = "Provider=SQLOLEDB;Data Source=" & My_SQL_Server & ";Initial Catalog=" & My_SQL_DB & ";Integrated Security=SSPI;"
	Set conn = CreateObject("ADODB.Connection")

	conn.Open constr

	If Err.Number <> 0 Then
		write_log Now() & " * Error * " & networkInfo.UserName & " * Open SQL Conn (" & Err.Description & ")"
		Set conn = Nothing
		exit sub
	end if

	conn.CommandTimeout = 0

	conn.Execute myquery

	If Err.Number <> 0 Then
		write_log Now() & " * Error * " & networkInfo.UserName & " * SQL Execute Query (" & Err.Description & ")"
		Set conn = Nothing
		exit sub
	end if
    
	conn.Close

	If Err.Number <> 0 Then
		write_log Now() & " * Error * " & networkInfo.UserName & " * SQL Close Con (" & Err.Description & ")"
	end if
    
	Set conn = Nothing
End Sub

Sub Execute_SQL(Filepath)
	Dim objFile, strLine

	if oFSO.fileexists(Filepath) then
		Set objFile = oFSO.OpenTextFile(Filepath)
		Do Until objFile.AtEndOfStream
			if len(strLine) > 0 then
				strLine= strLine & vbcrlf & objFile.ReadLine
			else
    				strLine= objFile.ReadLine
			end if
		Loop
		objfile.close

		Append_ODS strLine
	else
		write_log Now() & " * Error * " & networkInfo.UserName & " * SQL Script (" & Filepath & ") does not exist"
	end if

	set objFile = Nothing
end sub

Sub Write_Log(ByVal text)
	Dim objFile, strLine

	if oFSO.fileexists(Log_Path) then
		Set objFile = oFSO.OpenTextFile(Log_Path)
		Do Until objFile.AtEndOfStream
			if len(strLine) > 0 then
				strLine= strLine & vbcrlf & objFile.ReadLine
			else
    				strLine= objFile.ReadLine
			end if
		Loop
		objfile.close
		Set objfile = oFSO.CreateTextFile(Log_Path,True)
		objfile.write strLine & vbcrlf & text
		objfile.close
	else

		Set objfile = oFSO.CreateTextFile(Log_Path,True)
		objfile.write text
		objfile.close
	end if

	set objFile = Nothing
End Sub

Function Ceil(x)
    If Round(x) = x Then
        Ceil = x
    Else
        Ceil = Round(x + 0.5)
    End If
End Function

Function IsArray(anArray)
    Dim I
    On Error Resume Next
    I = UBound(anArray, 1)
    If Err.Number = 0 Then
        IsArray = True
    Else
        IsArray = False
    End If
End Function
