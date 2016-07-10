### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

###
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.
###

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
$FinalName = "@final.name@"


###############################################################################
###
### Installs tez component.
###
### Arguments:
###     component: Component to be installed, it should be tez
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: tez
###
###############################################################################
function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $role
    )
{
    if ( $component -eq "tez" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

	    ### $tezInstallPath: the name of the folder containing the application, after unzipping
	    $tezInstallPath = Join-Path $nodeInstallRoot $FinalName

	    Write-Log "Installing Apache tez @final.name@ to $tezInstallPath"

        ### Create Node Install Root directory
        if( -not (Test-Path "$nodeInstallRoot"))
        {
            Write-Log "Creating Node Install Root directory: `"$nodeInstallRoot`""
            New-Item -ItemType directory -Path  "$nodeInstallRoot"
        }
        ### Create Tez Install Root directory
        if( -not (Test-Path "$tezInstallPath"))
        {
            Write-Log "Creating Node Install Root directory: `"$tezInstallPath`""
            New-Item -ItemType directory -Path  "$tezInstallPath"
        }


        ###
        ###  Unzip Hadoop distribution from compressed archive
        ###
        Write-Log "Extracting $FinalName.zip to $tezInstallPath"
        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$FinalName-minimal.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$tezInstallPath`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$FinalName-minimal.zip")
            $destinationFolder = $shellApplication.NameSpace($tezInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

        ### Create Lib dir if necessary
        $targetdir = "$tezInstallPath"
        if( -not (Test-Path -Path  "$targetdir"))
        {
            Write-Log "Creating Lib Install directory: `"$targetdir`""
            New-Item -ItemType directory -Path  "$targetdir"
        }

        ### Copy actual .tar.gz file of tez content, for other apps, to $TEZ_HOME\conf.
        Write-Log "Creating `"$HDP_RESOURCES_DIR\$FinalName.tar.gz`" to `"$targetdir`""
        $xcopy_cmd = "xcopy /EIYF `"$HDP_RESOURCES_DIR\$FinalName.tar.gz`" `"$targetdir`""
        Invoke-Cmd $xcopy_cmd

        ###
        ###  Copy template config files
        ###
        $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\*.xml`" `"$tezInstallPath\conf`""
        Invoke-Cmd $xcopy_cmd

        ###
        ###  Remove slf4j.jar from the install, because of BUG-33641
        ###
        $slf4jjar = $tezInstallPath + "\lib\slf4j-log4j*.jar"
        Remove-Item $slf4jjar -ErrorAction SilentlyContinue -Force

        ###
        ### Set TEZ_HOME environment variable
        ###
        Write-Log "Setting the TEZ_HOME environment variable at machine scope to `"$tezInstallPath`""
        [Environment]::SetEnvironmentVariable("TEZ_HOME", $tezInstallPath, [EnvironmentVariableTarget]::Machine)
        $ENV:TEZ_HOME = "$tezInstallPath"
        $tezClassPath = $tezInstallPath + "\conf\;" + $tezInstallPath + "\*;" + $tezInstallPath + "\lib\*"
        Write-Log "Setting the TEZ_CLASSPATH environment variable at machine scope to `"$tezClassPath`""
        [Environment]::SetEnvironmentVariable("TEZ_CLASSPATH", $tezClassPath, [EnvironmentVariableTarget]::Machine)
        $ENV:TEZ_CLASSPATH = "$tezClassPath"
        
        #Configuring the tez.lib.uris
        Configure "tez" $nodeInstallRoot $serviceCredential @{
        "tez.lib.uris" = "/apps/tez/$FinalName.tar.gz"
        }



        Write-Log "Finished installing Apache tez"
    }
    else
    {
        throw "Install: Unsupported compoment argument."
    }
}

###############################################################################
###
### Uninstalls tez component.
###
### Arguments:
###     component: Component to be uninstalled.
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################
function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot
    )
{
    if ( $component -eq "tez" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        Write-Log "Uninstalling Apache tez $FinalName"
        $tezInstallPath = Join-Path $nodeInstallRoot $FinalName

        ### If Hadoop Core root does not exist exit early
        if ( -not (Test-Path $tezInstallPath) )
        {
            return
        }

        ###
        ### Delete install dir
        ###
        $cmd = "rd /s /q `"$tezInstallPath`""
        Invoke-Cmd $cmd

        ### Removing TEZ_HOME environment variable
        Write-Log "Removing the TEZ_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "TEZ_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Removing the TEZ_CLASSPATH environment variable"
        [Environment]::SetEnvironmentVariable( "TEZ_CLASSPATH", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Successfully uninstalled tez"

    }
    else
    {
        throw "Uninstall: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the tez component.
###
### Arguments:
###     component: Component to be configured, it should be "tez"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs:
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{

    if ( $component -eq "tez" )
    {
        Write-Log "Starting Tez configuration"
        $xmlFile = "$ENV:TEZ_HOME\conf\tez-site.xml"
        UpdateXmlConfig $xmlFile $configs
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "tez" )
    {
        Write-Log "StartService: tez does not have any services"
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "tez" )
    {
        Write-Log "StopService: tez does not have any services"
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

### Helper routine that updates the given fileName XML file with the given
### key/value configuration values. The XML file is expected to be in the
### Hadoop format. For example:
### <configuration>
###   <property>
###     <name.../><value.../>
###   </property>
### </configuration>
function UpdateXmlConfig(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName,
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    $xml = New-Object System.Xml.XmlDocument
    $xml.PreserveWhitespace = $true
    $xml.Load($fileName)

    foreach( $key in empty-null $config.Keys )
    {
        $value = $config[$key]
        $found = $False
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $_.value = $value; $found = $True }
        if ( -not $found )
        {
            $newItem = $xml.CreateElement("property")
            $newItem.AppendChild($xml.CreateSignificantWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("name")) | Out-Null
            $newItem.AppendChild($xml.CreateSignificantWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("value")) | Out-Null
            $newItem.AppendChild($xml.CreateSignificantWhitespace("`r`n  ")) | Out-Null
            $newItem.name = $key
            $newItem.value = $value
            $xml["configuration"].AppendChild($xml.CreateSignificantWhitespace("`r`n  ")) | Out-Null
            $xml["configuration"].AppendChild($newItem) | Out-Null
            $xml["configuration"].AppendChild($xml.CreateSignificantWhitespace("`r`n")) | Out-Null
        }
    }
    $xml.Save($fileName)
    $xml.ReleasePath
}

### Helper routine that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
