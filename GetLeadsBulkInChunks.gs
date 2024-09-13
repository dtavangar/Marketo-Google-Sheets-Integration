/**
 * Marketo Bulk Extract Script with UID Tracking
 * 
 * Description:
 * This script leverages Marketo's Bulk Extract API to incrementally export lead data into Google Sheets, 
 * ensuring that previously downloaded records are not redownloaded. It tracks the highest lead UID (Marketo ID) 
 * and uses that as a checkpoint for the next download, ensuring data consistency across runs.
 * 
 * Author: Damon Tavangar
 * Email: tavangar2017@gmail.com
 * 
 * Version: 1.1
 * License: GPL License
 */


// Marketo API Credentials
var CLIENT_ID = '<YOUR CLIENT ID>';
var CLIENT_SECRET = '<YOUR CLIENT SECRET>';
var MUNCHKIN_ID = '<YOUR MUNCHKIN_ID>';
var SHEET_ID = "<GOOGLE SHEET ID>"; // Define the sheet ID here

// Main function to run bulk export in chunks
function runBulkExportInChunks() {
  var properties = PropertiesService.getScriptProperties();
  var startTime = new Date().getTime();
  var lastMaxUID = properties.getProperty('lastMaxUID'); // Retrieve last max UID
  var startUID = lastMaxUID ? parseInt(lastMaxUID, 10) : 0; // Start at 0 if no UID is stored
  var token = getAccessToken();

  while (true) {
    Logger.log("Processing chunk starting from UID: " + startUID);

    // Check queued jobs and stop if limit exceeded
    if (getNumberOfQueuedJobs(token) >= 10) {
      Logger.log('Too many jobs in queue, stopping execution. Will resume in the next run.');
      return;
    }

    var success = createBulkExportJobForUIDRange(startUID, token);

    if (!success) {
      Logger.log('Error occurred, stopping execution.');
      return;
    }

    // Exit after some time to avoid script timeout
    var elapsedTime = (new Date().getTime() - startTime) / 1000; // in seconds
    if (elapsedTime > 300) { // Stop after 5 minutes of execution
      Logger.log('Stopping script to avoid timeout, will resume with the next chunk.');
      return;
    }
  }

  Logger.log('All chunks processed successfully.');
}

// Function to check the number of queued jobs
function getNumberOfQueuedJobs(token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export.json?access_token=' + token;

  var response = UrlFetchApp.fetch(url);
  var jsonResponse = JSON.parse(response.getContentText());

  if (jsonResponse.success) {
    var queuedJobs = jsonResponse.result.filter(function(job) {
      return job.status === 'Queued' || job.status === 'Processing';
    });

    Logger.log('Number of queued jobs: ' + queuedJobs.length);
    return queuedJobs.length;
  } else {
    Logger.log('Error fetching export jobs: ' + jsonResponse.errors[0].message);
    return 0;
  }
}

// Function to create the bulk export job for a range of UIDs
function createBulkExportJobForUIDRange(startUID, token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/create.json?access_token=' + token;

  var payload = {
    "format": "CSV",
    "filter": {
      "id": {
        "$gt": startUID // Fetch records with UIDs greater than the last processed UID
      }
    },
    "fields": ["id", "email", "createdAt", "updatedAt"],
    "batchSize": chunkSize // Limit batch size to chunkSize
  };

  var options = {
    'method': 'post',
    'contentType': 'application/json',
    'payload': JSON.stringify(payload)
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    var jsonResponse = JSON.parse(response.getContentText());

    if (jsonResponse.success) {
      var exportId = jsonResponse.result[0].exportId;
      Logger.log('Bulk Export Job Created with exportId: ' + exportId);
      enqueueBulkExportJob(exportId, token);

      // Save the export ID for later status checks
      saveExportId(exportId);

      return true;
    } else {
      Logger.log('Error creating bulk export job: ' + jsonResponse.errors[0].message);
      return false;
    }
  } catch (e) {
    Logger.log('Exception occurred while creating the export job: ' + e.message);
    return false;
  }
}

// Function to save exportId for later status checks
function saveExportId(exportId) {
  var properties = PropertiesService.getScriptProperties();
  var exportIds = properties.getProperty('exportIds');

  if (exportIds) {
    exportIds = JSON.parse(exportIds);
  } else {
    exportIds = [];
  }

  exportIds.push(exportId);
  properties.setProperty('exportIds', JSON.stringify(exportIds));
}

// Function to enqueue the export job
function enqueueBulkExportJob(exportId, token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/' + exportId + '/enqueue.json?access_token=' + token;

  var options = {
    'method': 'post',
    'contentType': 'application/json'
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    var jsonResponse = JSON.parse(response.getContentText());

    if (jsonResponse.success) {
      Logger.log('Bulk Export Job Enqueued: ' + exportId);
    } else {
      Logger.log('Error enqueuing bulk export job: ' + jsonResponse.errors[0].message);
    }
  } catch (e) {
    Logger.log('Exception occurred while enqueuing the export job: ' + e.message);
  }
}

// Function to check the status of export jobs and download the completed ones
function checkBulkExportStatusAndDownload() {
  var properties = PropertiesService.getScriptProperties();
  var exportIds = properties.getProperty('exportIds');

  if (exportIds) {
    exportIds = JSON.parse(exportIds);
  } else {
    Logger.log('No export jobs found.');
    return;
  }

  var token = getAccessToken();

  for (var i = 0; i < exportIds.length; i++) {
    var exportId = exportIds[i];
    var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/' + exportId + '/status.json?access_token=' + token;

    try {
      var response = UrlFetchApp.fetch(url);
      var jsonResponse = JSON.parse(response.getContentText());

      if (jsonResponse.success) {
        var status = jsonResponse.result[0].status;
        Logger.log('Export Job Status for ' + exportId + ': ' + status);

        if (status === 'Completed') {
          downloadBulkExtractToGoogleSheets(exportId, token); // Download and write to Google Sheets
          // Remove completed job from the list
          exportIds.splice(i, 1);
          i--; // Adjust loop index after removing
        } else if (status === 'Queued' || status === 'Processing') {
          Logger.log('Job ' + exportId + ' is still in progress, will check again in the next scheduled execution.');
        }
      } else {
        Logger.log('Error checking export job status: ' + jsonResponse.errors[0].message);
        if (jsonResponse.errors[0].message.includes("not found")) {
          // If the job is not found, log it and remove it from the list
          Logger.log('Export job ' + exportId + ' not found, removing from the list.');
          exportIds.splice(i, 1);  // Remove from the list
          i--; // Adjust the loop index
        }
      }
    } catch (e) {
      Logger.log('Exception occurred while checking export job status for ' + exportId + ': ' + e.message);
    }
  }

  // Save updated list of export IDs
  properties.setProperty('exportIds', JSON.stringify(exportIds));
}

// Function to download and save the bulk extract to Google Sheets
function downloadBulkExtractToGoogleSheets(exportId, token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/' + exportId + '/file.json?access_token=' + token;

  try {
    var response = UrlFetchApp.fetch(url);
    var csvContent = response.getContentText();
    
    if (response.getResponseCode() === 200) {
      Logger.log('Bulk Extract File Content: ' + csvContent);
      
      // Process the CSV and write it to Google Sheets
      var sheet = SpreadsheetApp.openById(SHEET_ID).getActiveSheet(); // Use the provided sheet ID
      var rows = Utilities.parseCsv(csvContent);

      var lastRow = sheet.getLastRow();
      var headersExist = lastRow > 0;

      // Collect existing IDs in advance (instead of checking one by one)
      var existingIds = new Set();
      if (lastRow > 1) {
        var idColumn = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
        for (var i = 0; i < idColumn.length; i++) {
          existingIds.add(idColumn[i][0]);
        }
      }

      // Write headers if they don't exist
      if (!headersExist && rows.length > 0) {
        sheet.appendRow(rows[0]); // Write headers
      }

      // Collect new rows to batch insert
      var newRows = [];
      var maxUID = 0;

      for (var i = 1; i < rows.length; i++) {
        var recordId = parseInt(rows[i][0], 10);
        if (!existingIds.has(recordId)) {
          newRows.push(rows[i]); // Collect the new row
          existingIds.add(recordId); // Track the new ID
          if (recordId > maxUID) {
            maxUID = recordId; // Update max UID
          }
        } else {
          Logger.log('Skipping duplicate record with ID: ' + recordId);
        }
      }

      if (newRows.length > 0) {
        sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows); // Batch insert
      }

      // Update the last max UID property
      if (maxUID > 0) {
        PropertiesService.getScriptProperties().setProperty('lastMaxUID', maxUID);
      }

      Logger.log('Data written to Google Sheets successfully.');

    } else {
      Logger.log('Error downloading bulk extract file: ' + response.getContentText());
    }
  } catch (e) {
    Logger.log('Exception occurred while downloading the bulk extract: ' + e.message);
  }
}

// Function to get access token from Marketo
function getAccessToken() {
  var tokenUrl = 'https://' + MUNCHKIN_ID + '.mktorest.com/identity/oauth/token?grant_type=client_credentials&client_id=' + CLIENT_ID + '&client_secret=' + CLIENT_SECRET;

  try {
    var response = UrlFetchApp.fetch(tokenUrl);
    var jsonResponse = JSON.parse(response.getContentText());

    if (jsonResponse.access_token) {
      Logger.log('Access token retrieved successfully.');
      return jsonResponse.access_token;
    } else {
      Logger.log('Error retrieving access token: ' + jsonResponse.error_description);
      throw new Error('Failed to retrieve access token');
    }
  } catch (e) {
    Logger.log('Error fetching access token, retrying: ' + e.message);
    Utilities.sleep(2000); // Retry after 2 seconds
    return getAccessToken(); // Retry logic (consider adding a retry count to prevent infinite loops)
  }
}
