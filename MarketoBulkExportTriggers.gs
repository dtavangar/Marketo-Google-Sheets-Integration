/**
 * Marketo Bulk Export Trigger Script
 * 
 * Description:
 * This script sets up time-based triggers to automate the process of creating bulk export jobs and 
 * checking the status of these jobs. It runs the export job every minute and checks the job status 
 * every 10 minutes to ensure timely downloads of completed exports.
 * 
 * Author: Damon Tavangar
 * Email: tavangar2017@gmail.com
 * 
 * Version: 1.0
 * License: GPLv3 License
 */


// Function to create a trigger for running the export job every minute
function createOneMinuteTriggerForJobCreation() {
  // Create a trigger to run every 1 minute
  ScriptApp.newTrigger('runBulkExportInChunks')
    .timeBased()
    .everyMinutes(1) // Set to run every 1 minute
    .create();
}

// Function to create a trigger for checking the export job status every 10 minutes
function createTenMinuteTriggerForStatusCheck() {
  // Create a trigger to run every 10 minutes
  ScriptApp.newTrigger('checkBulkExportStatusAndDownload')
    .timeBased()
    .everyMinutes(10) // Set to run every 10 minutes
    .create();
}
