Imports System.Xml
Imports System.IO
Module Module1
    Sub Main()
        Dim drp As String
        Dim drpval As String
        Dim version As String
        Dim reportfolder As String
        reportfolder = Directory.GetCurrentDirectory().ToString() + "\ReportBuild"
        Console.WriteLine("----------------------------------------------------------------------")
        Console.WriteLine("Enter the Release Version for RDL")
        version = Console.ReadLine()
        drp = "textbox"
        Console.WriteLine("To enable textbox enter 1, To enable dropwdown enter 2")
        drpval = Console.ReadLine()
        If drpval = "1" Then
            drp = "textbox"
        ElseIf drpval = "2" Then
            drpval = "dropdown"
        Else
            drp = "textbox"
        End If
        Try
            DirSearch(reportfolder, drp, version)
        Catch ex As Exception
            Console.WriteLine(ex.ToString())
        End Try
        Console.WriteLine("----------------------------------------------------------------------")
        Console.WriteLine("RDL Converted successfully!!!")
        Console.ReadKey()
    End Sub

    Private Sub replace(ByVal filePath As String, ByVal param As String)
        Dim add As Boolean = False
        If filePath = String.Empty Then
            Exit Sub
        End If
        Dim doc As XmlDocument = New XmlDocument()
        Try
            doc.Load(filePath)
        Catch ex As Exception
            Throw New Exception(ex.Message, ex.InnerException)
            Exit Sub
        End Try
        Dim node As XmlNode
        Dim node2 As XmlNode
        Dim node3 As XmlNode
        Dim nodeList As XmlNodeList
        Dim nodeList2 As XmlNodeList
        Dim nodeList3 As XmlNodeList
        Dim root As XmlNode = doc.DocumentElement
        nodeList = root.ChildNodes
        For Each node In nodeList
            If node.Name = "ReportParameters" Then
                nodeList2 = node.ChildNodes
                For Each node2 In nodeList2
                    If param = "textbox" Then
                        If node2.Attributes.ItemOf("Name").Value.ToString() = "IP_PAYOR" Or node2.Attributes.ItemOf("Name").Value.ToString() = "IP_GROUP" Or node2.Attributes.ItemOf("Name").Value.ToString() = "IP_EMPLOYER" Then
                            nodeList3 = node2.ChildNodes
                            For Each node3 In nodeList3
                                add = True
                                If node3.Name = "Hidden" Then
                                    add = False
                                    Exit For
                                End If
                            Next
                            If add Then
                                node2.InnerXml = node2.InnerXml & "<Hidden>true</Hidden>"
                            End If
                        End If
                    ElseIf param = "dropdown" Then
                        If node2.Attributes.ItemOf("Name").Value.ToString() = "IP_PAYORTEXT" Or node2.Attributes.ItemOf("Name").Value.ToString() = "IP_GROUPTEXT" Or node2.Attributes.ItemOf("Name").Value.ToString() = "IP_EMPLOYERTEXT" Then
                            nodeList3 = node2.ChildNodes
                            For Each node3 In nodeList3
                                add = True
                                If node3.Name = "Hidden" Then
                                    add = False
                                    Exit For
                                End If
                            Next
                            If add Then
                                node2.InnerXml = node2.InnerXml & "<Hidden>true</Hidden>"
                            End If
                        End If
                    End If
                Next
            End If
        Next
        doc.Save(filePath)
    End Sub
    Private Sub DirSearch(ByVal sDir As String, ByVal param As String, ByVal version As String)
        For Each d As String In Directory.GetDirectories(sDir)
            For Each f As String In Directory.GetFiles(d)
                If InStr(f, ".rdl") > 0 Then
                    System.IO.File.WriteAllText(f, System.IO.File.ReadAllText(f).Replace("<Value>Release</Value>", "<Value>" + version + "</Value>"))
                    replace(f, param)
                End If
            Next
            DirSearch(d, param, version)

        Next
    End Sub
End Module
