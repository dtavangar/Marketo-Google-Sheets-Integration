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

// Function to create a trigger for running the export job every 5 minutes
function createFiveMinuteTriggerForJobCreation() {
  // Create a trigger to run every 5 minutes
  ScriptApp.newTrigger('runBulkExportInChunks')
    .timeBased()
    .everyMinutes(5) // Set to run every 5 minutes
    .create();
}

// Function to create a trigger for checking the export job status every 15 minutes
function createFifteenMinuteTriggerForStatusCheck() {
  // Create a trigger to run every 15 minutes
  ScriptApp.newTrigger('checkBulkExportStatusAndDownload')
    .timeBased()
    .everyMinutes(15) // Set to run every 15 minutes
    .create();
}
