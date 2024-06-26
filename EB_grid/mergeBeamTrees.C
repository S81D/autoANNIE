#include <TFile.h>
#include <TTree.h>
#include <TChain.h>
#include <iostream>
#include <TSystem.h>
#include <TSystemDirectory.h>
#include <TList.h>
#include <TSystemFile.h>

// Usage: root -l -q 'mergeBeamTrees.C("<directory_path>", <run_number>)'

// Function to merge BeamTree trees
void mergeBeamTrees(const std::string &directory, int runNumber) {
    // Define the file directory based on the run number and output file name
    std::string runDirectory = directory + "/" + std::to_string(runNumber) + "/";
    std::string outputFileName = "beamfetcher_" + std::to_string(runNumber) + ".root";
    
    // Create a TChain to merge BeamTree trees
    TChain chain("BeamTree");

    // Create directory object
    TSystemDirectory dir(runDirectory.c_str(), runDirectory.c_str());
    TList *files = dir.GetListOfFiles();
    
    if (files) {
        TSystemFile *file;
        TString fname;
        TIter next(files);
        while ((file = (TSystemFile*)next())) {
            fname = file->GetName();
            // Check if the file name meets the criteria
            if (!file->IsDirectory() && fname.BeginsWith("beamfetcher_") && fname.EndsWith(".root")) {
                std::string filePath = runDirectory + fname.Data();
                std::cout << "Adding file: " << filePath << std::endl;
                chain.Add(filePath.c_str());
            }
        }
    }

    // Create a new ROOT file and save the merged tree
    TFile outputFile(outputFileName.c_str(), "RECREATE");
    TTree *mergedTree = chain.CloneTree();
    mergedTree->Write();

    // Write and close the file
    outputFile.Close();
    
    std::cout << "Merged tree saved to " << outputFileName << std::endl;
}

// Wrapper function to merge BeamTree trees with provided directory path and run number
void mergeBeamTreesWrapper(const std::string &directory, int runNumber) {
    mergeBeamTrees(directory, runNumber);
}

// Entry point for the ROOT macro
void mergeBeamTrees(const char* dir, int runNumber) {
    mergeBeamTreesWrapper(std::string(dir), runNumber);
}