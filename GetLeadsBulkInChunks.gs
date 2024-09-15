/**
 * Marketo Bulk Extract to Google Sheets Script
 * 
 * This script automates the export of leads data from Marketo to Google Sheets 
 * using the Marketo Bulk API. It runs in chunks based on a date range, checks the 
 * status of export jobs, and ensures that duplicate data is not processed across 
 * script executions.
 * 
 * Key Features:
 * - Fetches leads data in chunks (31-day chunks in this case)
 * - Saves and resumes the last processed date to avoid duplicate processing
 * - Handles API retries for export jobs not found (404 errors)
 * - Writes the leads data to Google Sheets while skipping duplicates
 * 
 * Author: Damon Tavangar
 * Email: tavangar2017@gmail.com
 * 
 * Version: 1.2
 * License: GPL License
 */


// Marketo API Credentials
var CLIENT_ID = '<YOUR CLIENT ID>';
var CLIENT_SECRET = '<YOUR CLIENT SECRET>';
var MUNCHKIN_ID = '<YOUR MUNCHKIN_ID>';
var SHEET_ID = "<GOOGLE SHEET ID>"; // Define the sheet ID here
var chunkSize = 31; // 31-day chunks

// Main function to run bulk export in chunks
function runBulkExportInChunks() {
  var properties = PropertiesService.getScriptProperties();
  var startTime = new Date().getTime();
  
  // Retrieve the last processed date from properties, or set to the earliest date
  var lastProcessedDate = properties.getProperty('lastProcessedDate');
  var startDate = lastProcessedDate ? new Date(lastProcessedDate) : new Date('2021-06-20T00:00:00Z');
  var endDate = new Date('2024-12-12T00:00:00Z');
  var token = getAccessToken();

  while (startDate < endDate) {
    var currentEndDate = new Date(startDate);
    currentEndDate.setDate(currentEndDate.getDate() + chunkSize - 1);

    if (currentEndDate > endDate) {
      currentEndDate = endDate;
    }

    var formattedStartAt = startDate.toISOString().split('.')[0] + "Z";
    var formattedEndAt = currentEndDate.toISOString().split('.')[0] + "Z";

    Logger.log("Processing chunk from: " + formattedStartAt + " to: " + formattedEndAt);

    // Check queued jobs and stop if limit exceeded
    if (getNumberOfQueuedJobs(token) >= 10) {
      Logger.log('Too many jobs in queue, stopping execution. Will resume in the next run.');
      return;
    }

    var success = createBulkExportJobForDateRange(formattedStartAt, formattedEndAt, token);

    if (!success) {
      Logger.log('Error occurred, stopping execution.');
      return;
    }

    // Retrieve leads data from the export job and pass it to getLatestCreatedAtFromChunk
    var exportId = getExportIdFromJob(token); // Get the exportId from the job created
    if (exportId) {
      var leadsData = retrieveLeadsFromExportJob(token, exportId); // Fetch the leads from the export job
      if (leadsData && leadsData.result) {
        var latestCreatedAt = getLatestCreatedAtFromChunk(leadsData);
        if (latestCreatedAt) {
          properties.setProperty('lastProcessedDate', latestCreatedAt);
        } else {
          Logger.log('No leads found in the chunk, skipping lastProcessedDate update.');
        }
      } else {
        Logger.log('Error retrieving leads data, skipping lastProcessedDate update.');
      }
    } else {
      Logger.log('Export ID is null, skipping lastProcessedDate update.');
    }

    startDate.setDate(startDate.getDate() + chunkSize);

    var elapsedTime = (new Date().getTime() - startTime) / 1000; // in seconds
    if (elapsedTime > 300) {
      Logger.log('Stopping script to avoid timeout, will resume with the next chunk.');
      return;
    }
  }

  properties.deleteProperty('lastProcessedDate');
  Logger.log('All chunks processed successfully.');
}

// Function to retrieve leads from the export job
function retrieveLeadsFromExportJob(token, exportId) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/' + exportId + '/file.json?access_token=' + token;
  var retries = 3;

  for (var attempt = 0; attempt < retries; attempt++) {
    try {
      var response = UrlFetchApp.fetch(url);
      var csvContent = response.getContentText();
      return Utilities.parseCsv(csvContent);
    } catch (e) {
      if (e.message.includes("404")) {
        Logger.log('Export job not found (404), retrying in 2 seconds.');
        Utilities.sleep(2000); // Wait before retrying
      } else {
        Logger.log('Error occurred: ' + e.message);
        throw e;
      }
    }
  }

  Logger.log('Failed after 3 retries, export job not found.');
  return null; // If all retries fail, return null
}




// Function to get exportId from the latest created export job
function getExportIdFromJob(token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export.json?access_token=' + token;
  var response = UrlFetchApp.fetch(url);
  var jsonResponse = JSON.parse(response.getContentText());

  if (jsonResponse.success) {
    // Assuming the latest export job is the last one in the list
    var exportJobs = jsonResponse.result;
    if (exportJobs.length > 0) {
      var latestJob = exportJobs[exportJobs.length - 1]; // Get the most recent job
      return latestJob.exportId;
    } else {
      Logger.log('No export jobs found.');
      return null;
    }
  } else {
    Logger.log('Error fetching export jobs: ' + jsonResponse.errors[0].message);
    return null;
  }
}

// Function to clear all properties
function clearAllProperties() {
  var properties = PropertiesService.getScriptProperties();
  properties.deleteAllProperties();
  Logger.log('All script properties cleared.');
}

// Function to get the latest createdAt date from the chunk
function getLatestCreatedAtFromChunk(apiResponse) {
  var leads = apiResponse.result; // Accessing the leads data from the API response

  // If there are no leads, return null or some default value
  if (leads.length === 0) {
    return null;
  }

  // Initialize the latest createdAt as the first lead's createdAt
  var latestCreatedAt = leads[0].createdAt;

  // Iterate through the leads and find the latest createdAt date
  for (var i = 1; i < leads.length; i++) {
    if (new Date(leads[i].createdAt) > new Date(latestCreatedAt)) {
      latestCreatedAt = leads[i].createdAt;
    }
  }

  Logger.log("Latest createdAt from chunk: " + latestCreatedAt);
  return latestCreatedAt;
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

// Function to create the bulk export job for the given date range
function createBulkExportJobForDateRange(startAt, endAt, token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/create.json?access_token=' + token;

  var payload = {
    "format": "CSV",
    "filter": {
      "createdAt": {
        "startAt": startAt,
        "endAt": endAt
      }
    },
    "fields": ["id", "email", "createdAt", "updatedAt"]
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
  var completedJobs = [];

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
          downloadBulkExtractToGoogleSheets(exportId, token);
          
          // Ensure there was actual lead data before removing from exportIds
          var fileContent = UrlFetchApp.fetch('https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/' + exportId + '/file.json?access_token=' + token).getContentText();
          var fileRows = Utilities.parseCsv(fileContent);
          if (fileRows.length > 1) {
            completedJobs.push(exportId); // Add to completedJobs list only if data was found
          } else {
            Logger.log('Job ' + exportId + ' completed but no lead data found.');
          }
        } else if (status === 'Queued' || status === 'Processing') {
          Logger.log('Job ' + exportId + ' is still in progress, will check again in the next scheduled execution.');
        }
      } else {
        Logger.log('Error checking export job status: ' + jsonResponse.errors[0].message);
        if (jsonResponse.errors[0].message.includes("not found")) {
          Logger.log('Export job ' + exportId + ' not found, removing from the list.');
          completedJobs.push(exportId); // Add to completedJobs if job not found
        }
      }
    } catch (e) {
      Logger.log('Exception occurred while checking export job status for ' + exportId + ': ' + e.message);
    }
  }

  // Remove completed jobs from the exportIds list
  exportIds = exportIds.filter(id => !completedJobs.includes(id));
  properties.setProperty('exportIds', JSON.stringify(exportIds));

  Logger.log('Remaining jobs: ' + exportIds.length);
  Logger.log('API Call made at: ' + new Date().toISOString());

}

// Function to download and save the bulk extract to Google Sheets
function downloadBulkExtractToGoogleSheets(exportId, token) {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/' + exportId + '/file.json?access_token=' + token;

  try {
    var response = UrlFetchApp.fetch(url);
    var csvContent = response.getContentText();
    var rows = Utilities.parseCsv(csvContent);

    // If there is no data or only headers, log it and stop processing
    if (rows.length <= 1 || rows[1].length === 0) {
      Logger.log('No lead data found for exportId ' + exportId + ', only headers returned.');
      return;
    }

    Logger.log('Bulk Extract File Content: ' + csvContent);
    
    // Process the CSV and write it to Google Sheets
    var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    var properties = PropertiesService.getScriptProperties();
    
    // Track existing IDs in the Google Sheet to avoid duplicates across runs
    var existingIds = new Set();
    var lastRow = sheet.getLastRow();

    // Retrieve the last processed ID from properties to avoid duplicates across script runs
    var lastProcessedId = properties.getProperty('lastProcessedId');
    
    if (lastRow > 0) {
      var idColumn = sheet.getRange(2, 1, lastRow - 1, 1).getValues(); // Assuming ID is in the first column
      for (var i = 0; i < idColumn.length; i++) {
        existingIds.add(idColumn[i][0]); // Add all existing IDs to the set
      }
    }

    // Append rows with unique IDs only
    for (var i = 0; i < rows.length; i++) {
      if (i === 0 && lastRow > 0) {
        // Skip header row if already exists
        continue;
      }

      var recordId = rows[i][0]; // Assuming ID is in the first column
      // Skip if ID is already processed or equals the last processed ID
      if (!existingIds.has(recordId) && recordId > lastProcessedId) {
        sheet.appendRow(rows[i]); // Append only if the ID is unique
        existingIds.add(recordId); // Add the new ID to the set
      } else {
        Logger.log('Skipping duplicate or already processed record with ID: ' + recordId);
      }
    }

    // Update the last processed ID to persist across script runs
    var latestProcessedId = rows[rows.length - 1][0]; // Assuming ID is in the first column
    properties.setProperty('lastProcessedId', latestProcessedId);
    
    Logger.log('Data written to Google Sheets successfully.');
  } catch (e) {
    Logger.log('Exception occurred while downloading the bulk extract: ' + e.message);
  }
}


// Function to get last processed CreatedAt date from Google Sheets
function getLastProcessedCreatedAtDate() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  var lastRow = sheet.getLastRow();
  
  if (lastRow > 1) { // Assuming first row is headers
    var lastDate = sheet.getRange(lastRow, 3).getValue(); // Assuming createdAt is in the third column
    if (lastDate) {
      return new Date(lastDate).toISOString().split('.')[0] + "Z";
    }
  }
  
  return null; // No previous data or no valid date found
}

// Function to get access token from Marketo
function getAccessToken() {
  var properties = PropertiesService.getScriptProperties();
  var tokenInfo = properties.getProperty('tokenInfo');

  if (tokenInfo) {
    tokenInfo = JSON.parse(tokenInfo);

    // Check if the token is still valid
    var now = new Date().getTime();
    if (now < tokenInfo.expires_at) {
      Logger.log('Using cached access token.');
      return tokenInfo.access_token;
    }
  }

  // If no token or expired, fetch a new one
  var tokenUrl = 'https://' + MUNCHKIN_ID + '.mktorest.com/identity/oauth/token?grant_type=client_credentials&client_id=' + CLIENT_ID + '&client_secret=' + CLIENT_SECRET;
  
  var response = UrlFetchApp.fetch(tokenUrl);
  var jsonResponse = JSON.parse(response.getContentText());

  if (jsonResponse.access_token) {
    Logger.log('Access token retrieved successfully.');

    // Cache the token with its expiration time (assume 1 hour)
    var expiresAt = new Date().getTime() + (jsonResponse.expires_in * 1000);
    properties.setProperty('tokenInfo', JSON.stringify({
      access_token: jsonResponse.access_token,
      expires_at: expiresAt
    }));

    return jsonResponse.access_token;
  } else {
    Logger.log('Error retrieving access token: ' + jsonResponse.error_description);
    throw new Error('Failed to retrieve access token');
  }
}
