/**
 * Marketo Bulk Extract Script with UID Tracking and Date Optimization
 * 
 * Description:
 * This script leverages Marketo's Bulk Extract API to incrementally export lead data into Google Sheets,
 * ensuring that previously downloaded records are not re-downloaded. It tracks the highest lead UID (Marketo ID)
 * and uses that as a checkpoint for the next download, ensuring data consistency across runs.
 * 
 * The script has been optimized to process lead data in 31-day chunks, respecting the Marketo Bulk Export API's 
 * 31-day limit on the `createdAt` filter and ensuring no more than 10 jobs are queued at a time.
 * 
 * Author: Damon Tavangar
 * Email: tavangar2017@gmail.com
 * 
 * Version: 1.3
 * License: GPL License
 */

// Marketo API Credentials
var CLIENT_ID = '<YOUR CLIENT ID>';
var CLIENT_SECRET = '<YOUR CLIENT SECRET>';
var MUNCHKIN_ID = '<YOUR MUNCHKIN_ID>';
var SHEET_ID = "<GOOGLE SHEET ID>"; // Define the Google Sheet ID here
var chunkSize = 31; // Marketo API limit of 31 fields per bulk request

/**
 * Main function to run bulk export in chunks.
 * This function orchestrates the entire export process, checking for queued jobs, creating new export jobs,
 * and ensuring data continuity by using the latest UID and `createdAt` date from Google Sheets.
 * 
 * The function ensures that only up to 10 jobs are queued at any time and splits the lead data into 
 * 31-day `createdAt` chunks to avoid hitting Marketo's 31-day filter limitation.
 */
function runBulkExportInChunks() {
  var properties = PropertiesService.getScriptProperties();
  var startTime = new Date().getTime(); // Track start time to manage script timeout
  var lastMaxUID = properties.getProperty('lastMaxUID'); // Retrieve last max UID from stored properties
  var startUID = lastMaxUID ? parseInt(lastMaxUID, 10) : 0; // Start at 0 if no UID is stored
  var token = getAccessToken(); // Fetch access token from Marketo
  var latestCreatedAt = getLatestCreatedAtFromSheet(); // Get the latest `createdAt` from Google Sheets

  Logger.log("Processing chunk starting from UID: " + startUID);

  // Process data in 31-day chunks until we've reached the present
  var success = createBulkExportJobForUIDRange(startUID, latestCreatedAt, token);

  if (!success) {
    Logger.log('Error occurred, stopping execution.');
    return;
  }

  Logger.log('All chunks processed successfully.');
}

/**
 * Function to get the latest `createdAt` timestamp from Google Sheets.
 * This function scans the Google Sheet for the latest `createdAt` date, so the script can filter the export
 * to only include leads created after the last export.
 * 
 * @return {Date|null} - Returns the latest `createdAt` timestamp or null if no data is available.
 */
function getLatestCreatedAtFromSheet() {
  var sheet = SpreadsheetApp.openById(SHEET_ID).getActiveSheet();
  var lastRow = sheet.getLastRow();

  if (lastRow < 2) { // No data or only header is present
    Logger.log('Google Sheet is empty or contains only headers, no date filter will be applied.');
    return null;
  }

  var createdAtColumn = sheet.getRange(2, 3, lastRow - 1, 1).getValues(); // Assuming `createdAt` is in column 3
  var latestCreatedAt = new Date(Math.max.apply(null, createdAtColumn.map(function(row) {
    return new Date(row[0]).getTime(); // Convert date strings to timestamps
  })));

  // Increment by 1 second to avoid re-downloading the last record
  latestCreatedAt.setSeconds(latestCreatedAt.getSeconds() + 1);

  Logger.log('Latest `createdAt` date found: ' + latestCreatedAt.toISOString());
  return latestCreatedAt;
}

/**
 * Function to create the bulk export job for a range of UIDs.
 * This function creates jobs using the Marketo API, splitting the `createdAt` date range into chunks
 * to respect Marketo's 31-day limit, and ensuring no more than 10 jobs are queued at a time.
 * 
 * @param {number} startUID - The starting UID for the export.
 * @param {Date|null} latestCreatedAt - The latest `createdAt` timestamp from the Google Sheet, if available.
 * @param {string} token - The Marketo access token.
 * @return {boolean} - Returns true if the job is successfully created, otherwise false.
 */
function createBulkExportJobForUIDRange(startUID, latestCreatedAt, token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/create.json?access_token=' + token;

  // If no `createdAt` timestamp is available, start from the beginning
  var startAt = latestCreatedAt ? latestCreatedAt : new Date(0); // Default to epoch if no timestamp
  var currentDate = new Date(); // Get current date for end range
  var queuedJobs = 0; // Track the number of jobs created in this run

  while (startAt < currentDate) {
    // Calculate the next end date chunk (31 days from the start date or current date)
    var endAt = new Date(startAt);
    endAt.setDate(endAt.getDate() + 31); // Add 31 days to start date
    if (endAt > currentDate) {
      endAt = currentDate; // Make sure we don't exceed the current date
    }

    // Check if we have space for more jobs in the queue (limit to 10)
    if (!checkJobQueueCapacity(token) || queuedJobs >= 10) {
      Logger.log('Too many jobs already created, stopping further job creation.');
      return false; // Stop if we've reached the limit of 10 jobs
    }

    // Build the filter with the `createdAt` range
    var filter = {
      "id": {
        "$gt": startUID // Fetch records with UIDs greater than the last processed UID
      },
      "createdAt": {
        "startAt": startAt.toISOString(), // Leads created after the last lead
        "endAt": endAt.toISOString()      // Up to the end of the 31-day chunk
      }
    };

    var payload = {
      "format": "CSV",
      "filter": filter,
      "fields": ["id", "email", "createdAt", "updatedAt"],
      "batchSize": chunkSize // Limit batch size to 31 (Marketo limit)
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
        enqueueBulkExportJob(exportId, token); // Enqueue the job after creation

        // Save the export ID for later status checks
        saveExportId(exportId);

        queuedJobs++; // Increment the number of jobs created in this run
      } else {
        Logger.log('Error creating bulk export job: ' + jsonResponse.errors[0].message);
        return false;
      }
    } catch (e) {
      Logger.log('Exception occurred while creating the export job: ' + e.message);
      return false;
    }

    // Move the `startAt` forward by 31 days for the next chunk
    startAt = endAt;
  }
  
  return true;
}

/**
 * Function to enqueue the export job.
 * This function enqueues a created export job using the Marketo API.
 * If there are too many jobs in the queue, it will log the error and stop the process.
 * 
 * @param {string} exportId - The ID of the export job.
 * @param {string} token - The Marketo access token.
 * @return {boolean} - Returns true if the job was enqueued successfully, false otherwise.
 */
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
      return true;
    } else {
      Logger.log('Error enqueuing bulk export job: ' + jsonResponse.errors[0].message);
      return false;
    }
  } catch (e) {
    Logger.log('Exception occurred while enqueuing the export job: ' + e.message);
    return false;
  }
}

/**
 * Function to check the status of export jobs and download the completed ones.
 * This function loops through all previously created export jobs, checks their status, and downloads the results
 * if the job is completed.
 */
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

/**
 * Function to download and save the bulk extract to Google Sheets.
 * This function downloads the results of a completed export job and writes the data into Google Sheets.
 * 
 * @param {string} exportId - The ID of the export job.
 * @param {string} token - The Marketo access token.
 */
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

/**
 * Function to get access token from Marketo.
 * This function fetches the OAuth access token required to authenticate API requests to Marketo.
 * 
 * @return {string} - The OAuth access token.
 */
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

/**
 * Function to check the number of queued jobs.
 * This function uses the Marketo API to check the number of currently queued or processing jobs.
 * If there are too many jobs (>=10), it will return false, indicating that no new jobs should be enqueued.
 * 
 * @param {string} token - The Marketo access token.
 * @return {boolean} - Returns true if there is space for more jobs, false if the queue is full (>= 10 jobs).
 */
function checkJobQueueCapacity(token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export.json?access_token=' + token;

  var response = UrlFetchApp.fetch(url);
  var jsonResponse = JSON.parse(response.getContentText());

  if (jsonResponse.success) {
    var queuedJobs = jsonResponse.result.filter(function(job) {
      return job.status === 'Queued' || job.status === 'Processing';
    });

    Logger.log('Number of queued jobs: ' + queuedJobs.length);

    // If there are 10 or more jobs, return false (queue is full)
    if (queuedJobs.length >= 10) {
      Logger.log('Too many jobs in the queue, stopping job creation.');
      return false;
    }

    // Otherwise, return true (there is capacity for more jobs)
    return true;
  } else {
    Logger.log('Error fetching export jobs: ' + jsonResponse.errors[0].message);
    return false; // Return false in case of error
  }
}

/**
 * Function to save the exportId for later status checks.
 * The function saves the exportId in the script's properties so that the status can be checked later.
 * 
 * @param {string} exportId - The ID of the export job.
 */
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
