@echo off

SETLOCAL

SET NUGET=%LocalAppData%\NuGet\NuGet.exe
SET FAKE=%LocalAppData%\FAKE\tools\Fake.exe
SET NYX=%LocalAppData%\Nyx\tools\build_next.fsx
SET GITVERSION=%LocalAppData%\GitVersion.CommandLine\tools\GitVersion.exe

echo Downloading NuGet.exe...
IF NOT EXIST %NUGET% @powershell -NoProfile -ExecutionPolicy unrestricted -Command "New-Item -ItemType directory -Path %LocalAppData%\NuGet\; (New-Object System.Net.WebClient).DownloadFile('https://dist.nuget.org/win-x86-commandline/latest/nuget.exe','%NUGET%')"

echo Downloading FAKE...
IF NOT EXIST %LocalAppData%\FAKE %NUGET% "install" "FAKE" "-OutputDirectory" "%LocalAppData%" "-ExcludeVersion" "-Version" "4.50.0"

echo Downloading GitVersion.CommandLine...
IF NOT EXIST %LocalAppData%\GitVersion.CommandLine %NUGET% "install" "GitVersion.CommandLine" "-OutputDirectory" "%LocalAppData%" "-ExcludeVersion" "-Version" "3.6.1"

echo Downloading Nyx...
%NUGET% "install" "Nyx" "-OutputDirectory" "%LocalAppData%" "-ExcludeVersion"

%FAKE% %NYX% "target=clean" -st
%FAKE% %NYX% "target=RestoreNugetPackages" -st

IF NOT [%1]==[] (set RELEASE_NUGETKEY="%1")

SET SUMMARY="Elders.Cronus.Persistence.Cassandra"
SET DESCRIPTION="Elders.Cronus.Persistence.Cassandra"

%FAKE% %NYX% appName=Elders.Cronus.Persistence.Cassandra appType=nuget appSummary=%SUMMARY% appDescription=%DESCRIPTION% nugetPackageName=Cronus.Persistence.Cassandra nugetkey=%RELEASE_NUGETKEY%
