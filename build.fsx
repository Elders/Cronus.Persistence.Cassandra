#I @"./bin/tools/FAKE/tools/"
#r @"./bin/tools/FAKE/tools/FakeLib.dll"
#r @"./bin/tools/Nuget.Core/lib/net40-Client/NuGet.Core.dll"

open System
open System.IO
open Fake
open Fake.Git
open Fake.FSharpFormatting
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper
open Fake.ProcessHelper

type System.String with member x.endswith (comp:System.StringComparison) str = x.EndsWith(str, comp)

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  BEGIN EDIT

let appName = getBuildParamOrDefault "appName" ""
let appType = getBuildParamOrDefault "appType" ""
let appSummary = getBuildParamOrDefault "appSummary" ""
let appDescription = getBuildParamOrDefault "appDescription" ""
let appAuthors = ["Nikolai Mynkow"; "Simeon Dimov";]

//  END EDIT
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

let buildDir  = @"./bin/Release" @@ appName
let releaseNotes = @"./src/" @@ appName @@ @"RELEASE_NOTES.md"
let release = LoadReleaseNotes releaseNotes

let nuget = environVar "NUGET"
let nugetWorkDir = "./bin/nuget" @@ appName

Target "Clean" (fun _ -> CleanDirs [buildDir; nugetWorkDir;])

Target "RestoreNugetPackages" (fun _ ->
  let packagesDir = @"./src/packages"
  !! "./src/*/packages.config"
  |> Seq.iter (RestorePackage (fun p ->
      { p with
          ToolPath = nuget
          OutputPath = packagesDir }))
)

Target "RestoreBowerPackages" (fun _ ->
    !! "./src/*/package.config"
    |> Seq.iter (fun config ->
        config.Replace("package.config", "")
        |> fun cfgDir ->
            printf "Bower working dir: %s" cfgDir
            let result = ExecProcess (fun info ->
                            info.FileName <- "cmd"
                            info.WorkingDirectory <- cfgDir
                            info.Arguments <- "/c npm install") (TimeSpan.FromMinutes 20.0)
            if result <> 0 then failwithf "'npm install' returned with a non-zero exit code")
)

Target "Build" (fun _ ->
  let appProjectFile = match appType with
                        | "msi" -> @"./src/" @@ appName + ".sln"
                        | _ -> @"./src/" @@ appName @@ appName + ".csproj"

  !! appProjectFile
      |> MSBuildRelease buildDir "Build"
      |> Log "Build-Output: "
)

Target "CreateWebNuGet" (fun _ ->
  let packages = [appName, appType]
  for appName,appType in packages do

      let nugetOutDir = nugetWorkDir
      let nugetOutArtifactsDir = nugetOutDir @@ "Artifacts"
      CleanDir nugetOutArtifactsDir

      //  Copy the build artifacts to the nuget pick dir
      match appType with
      | "web" -> CopyDir nugetOutArtifactsDir (buildDir @@ "_PublishedWebsites" @@ appName) allFiles
      | _ -> CopyDir nugetOutArtifactsDir buildDir allFiles

      //  Copy the deployment files if any to the nuget pick dir.
      let depl = @".\src\" @@ appName @@ @".\deployment\"
      if TestDir depl then XCopy depl nugetOutDir

      let nuspecFile = appName + ".nuspec"
      let nugetAccessKey =
          match appType with
          | "nuget" -> getBuildParamOrDefault "nugetkey" ""
          | _ ->  ""

      let nugetPackageName = getBuildParamOrDefault "nugetPackageName" appName
      let nugetDoPublish = nugetAccessKey.Equals "" |> not
      let nugetPublishUrl = getBuildParamOrDefault "nugetserver" "https://nuget.org"

      //  Create/Publish the nuget package
      NuGet (fun app ->
          {app with
              NoPackageAnalysis = true
              Authors = appAuthors
              Project = nugetPackageName
              Description = appDescription
              Version = release.NugetVersion
              Summary = appSummary
              ReleaseNotes = release.Notes |> toLines
              AccessKey = nugetAccessKey
              Publish = nugetDoPublish
              PublishUrl = nugetPublishUrl
              ToolPath = nuget
              OutputPath = nugetWorkDir
              WorkingDir = nugetWorkDir
          }) nuspecFile
)

Target "CreateLibraryNuGet" (fun _ ->
  let packages = [appName, appType]
  for appName,appType in packages do

      //  Exclude libraries which are part of the packages.config file only when nuget package is created.
      let nugetPackagesFile = "./src/" @@ appName @@ "packages.config"
      let dependencies = getDependencies nugetPackagesFile
      let dependencyFiles = dependencies
                            |> Seq.map(fun (name,ver) -> name + "." + ver)
                            |> Seq.collect(fun pkgName -> !! ("./src/packages/*/" + pkgName + ".nupkg"))
                            |> Seq.collect(fun pkg -> global.NuGet.ZipPackage(pkg).GetFiles())
                            |> Seq.map(fun file -> "\\" + filename file.Path)
                            |> fun gga -> Collections.Set(gga)
                            |> Set.toList

      let nugetOutDir = nugetWorkDir @@ "lib" @@ "net45-full"
      let excludePaths (pathsToExclude : string list) (path: string) = pathsToExclude |> List.exists (path.endswith StringComparison.OrdinalIgnoreCase)|> not
      let exclude = excludePaths dependencyFiles
      CopyDir nugetOutDir buildDir exclude

      let nuspecFile = appName + ".nuspec"
      let nugetAccessKey = getBuildParamOrDefault "nugetkey" ""
      let nugetPackageName = getBuildParamOrDefault "nugetPackageName" appName
      let nugetDoPublish = nugetAccessKey.Equals "" |> not
      let nugetPublishUrl = getBuildParamOrDefault "nugetserver" "https://nuget.org"


      //  Create/Publish the nuget package
      NuGet (fun app ->
          {app with
              NoPackageAnalysis = true
              Authors = appAuthors
              Project = nugetPackageName
              Description = appDescription
              Version = release.NugetVersion
              Summary = appSummary
              ReleaseNotes = release.Notes |> toLines
              Dependencies = dependencies
              AccessKey = nugetAccessKey
              Publish = nugetDoPublish
              PublishUrl = nugetPublishUrl
              ToolPath = nuget
              OutputPath = nugetWorkDir
              WorkingDir = nugetWorkDir
          }) nuspecFile
)



Target "Release" (fun _ ->
    StageAll ""
    let notes = String.concat "; " release.Notes
    Commit "" (sprintf "%s" notes)
    Branches.push ""

    Branches.tag "" release.NugetVersion
    Branches.pushTag "" "origin" release.NugetVersion
)

"Clean"
    ==> "RestoreNugetPackages"
    ==> "RestoreBowerPackages"
    ==> "Build"
    =?> ("CreateLibraryNuGet", appType.Equals "nuget")
    =?> ("CreateWebNuGet", appType.Equals "web")
    ==> "Release"

RunParameterTargetOrDefault "target" "Build"
