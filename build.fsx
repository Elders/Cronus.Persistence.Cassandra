#I @"./bin/tools/FAKE/tools/"
#r @"./bin/tools/FAKE/tools/FakeLib.dll"

open System
open System.IO
open Fake
open Fake.Git
open Fake.FSharpFormatting
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper

type System.String with member x.endswith (comp:System.StringComparison) str =
                          let newVal = x.Remove(x.Length-4)
                          newVal.EndsWith(str, comp)

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
let nugetOutDir = nugetWorkDir @@ "lib" @@ "net45-full"

Target "Clean" (fun _ -> CleanDirs [buildDir; nugetOutDir;])

Target "RestorePackages" (fun _ ->
  let packagesDir = @"./src/packages"
  !! "./**/packages.config"
  |> Seq.iter (RestorePackage (fun p ->
      { p with
          ToolPath = nuget
          OutputPath = packagesDir }))
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
      let nugetDependenciesFlat =
        match fileExists nugetPackagesFile with
        | true -> getDependencies nugetPackagesFile |> List.unzip |> fst
        | _ -> []

      let excludePaths (pathsToExclude : string list) (path: string) = pathsToExclude |> List.exists (path.endswith StringComparison.OrdinalIgnoreCase)|> not
      let exclude = excludePaths nugetDependenciesFlat
      CopyDir nugetOutDir buildDir exclude

      let nuspecFile = appName + ".nuspec"
      let nugetAccessKey = getBuildParamOrDefault "nugetkey" ""
      let nugetPackageName = getBuildParamOrDefault "nugetPackageName" appName
      let nugetDoPublish = nugetAccessKey.Equals "" |> not
      let nugetPublishUrl = getBuildParamOrDefault "nugetserver" "https://nuget.org"
      let dep = getDependencies nugetPackagesFile
      Console.WriteLine dep

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
              Dependencies = dep
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
    ==> "RestorePackages"
    ==> "Build"
    =?> ("CreateLibraryNuGet", appType.Equals "nuget")
    =?> ("CreateWebNuGet", appType.Equals "web")
    ==> "Release"

RunParameterTargetOrDefault "target" "Build"
