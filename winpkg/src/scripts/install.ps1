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
param(
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=0, Mandatory=$true )]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=0, Mandatory=$true )]
    $username,
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=1, Mandatory=$true )]
    $password,
    [String]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=1, Mandatory=$true )]
    $passwordBase64,
    [Parameter( ParameterSetName='CredentialFilePath', Mandatory=$true )]
    $credentialFilePath
    )

function Main( $scriptDir )
{
    Write-Log "Installing Apache Tez @final.name@ to $tezInstallPath"
    $FinalName = "@final.name@"
    
    ###
    ### Create the Credential object from the given username and password or the provided credentials file
    ###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
    Write-Log "Username: $username"
    Write-Log "CredentialFilePath: $credentialFilePath"       
   
    Install "Tez" $ENV:HADOOP_NODE_INSTALL_ROOT
    Write-Log "Configuring Apache Tez"
    $config = @{"tez.lib.uris"="hdfs://"+$ENV:NAMENODE_HOST+":8020/apps/tez/" + $FinalName + ".tar.gz"}
    if ((Test-Path ENV:HA) -and ($ENV:HA -ieq "yes")) 
    {
        $config = @{"tez.lib.uris"="hdfs://"+$ENV:NN_HA_CLUSTER_NAME+"/apps/tez/" + $FinalName + ".tar.gz"}
        $config["tez.am.max.app.attempts"] = "20"
    }
    if ((Test-Path ENV:ENABLE_LZO) -and ($ENV:ENABLE_LZO -ieq "yes"))
    {
        $hadoopLzoJar = @(gci -Filter hadoop-lzo*.jar -Path "$ENV:HADOOP_HOME\share\hadoop\common")[0]
        Write-Log "Creating tez.cluster.additional.classpath.prefix to point to $hadoopLzoJar.FullName"
        $config["tez.cluster.additional.classpath.prefix"] = $hadoopLzoJar.FullName
    }
    Configure "Tez" $ENV:HADOOP_NODE_INSTALL_ROOT $serviceCredential $config

    Write-Log "Finished installing Apache Tez"
}

try
{
    $scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
    $utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("TEZ") -PassThru
    $apiModule = Import-Module -Name "$scriptDir\InstallApi.psm1" -PassThru
    Main $scriptDir
}
catch
{
	Write-Log $_.Exception.Message "Failure" $_
	exit 1
}
finally
{
    if( $apiModule -ne $null )
    {
        Remove-Module $apiModule
    }

    if( $utilsModule -ne $null )
    {
        Remove-Module $utilsModule
    }
}
