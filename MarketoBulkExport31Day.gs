/**
 * Marketo Bulk Extract to Google Sheets or Google Drive
 * 
 * Description:
 * This script initiates a bulk export from Marketo for a user-defined 31-day window, monitors the job's progress, 
 * and saves the resulting CSV file either to Google Drive or Google Sheets (based on your preference).
 * The script supports error handling, retries for failed API calls, and improved logging for monitoring.
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

// Function to Get Access Token
function getAccessToken() {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/identity/oauth/token?grant_type=client_credentials&client_id=' + CLIENT_ID + '&client_secret=' + CLIENT_SECRET;
  var response = retryableFetch(url, { 'method': 'post' });
  var result = JSON.parse(response.getContentText());
  return result.access_token;
}

// Function to Start Bulk Export Job for Leads
function startBulkExtract(startDate) {
  try {
    var token = getAccessToken();
    var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/create.json?access_token=' + token;

    // Calculate the 31-day date range dynamically
    var dateRange = getDateRange(startDate);

    // Define the export job parameters
    var payload = {
      "format": "CSV",
      "filter": {
        "createdAt": {
          "startAt": dateRange.startAt, 
          "endAt": dateRange.endAt
        }
      },
      "fields": ["id", "firstName", "lastName", "email", "createdAt", "updatedAt"] // fields to extract
    };
    
    // Make the API request to start the bulk export job
    var response = retryableFetch(url, {
      'method': 'post',
      'headers': {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      'payload': JSON.stringify(payload)
    });

    // Log the response to see the job status
    var jsonResponse = JSON.parse(response.getContentText());
    Logger.log('Bulk Export Job Response: ' + JSON.stringify(jsonResponse));

    if (jsonResponse.success) {
      Logger.log('Bulk Export Job Created: ' + jsonResponse.result[0].exportId);
      return jsonResponse.result[0].exportId; // Return the export job ID for tracking
    } else {
      Logger.log('Error creating bulk export job: ' + jsonResponse.errors[0].message);
    }
  } catch (error) {
    Logger.log('Error starting bulk export job: ' + error);
  }
}

// Function to Check Bulk Export Job Status
function checkBulkExportStatus(exportId) {
  try {
    var token = getAccessToken();
    var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/bulk/v1/leads/export/' + exportId + '/status.json?access_token=' + token;
    
    // Make the API request to check the export job status
    var response = retryableFetch(url, {
      'headers': {
        'Authorization': 'Bearer ' + token
      }
    });
    
    var jsonResponse = JSON.parse(response.getContentText());
    Logger.log('Bulk Export Status Response: ' + JSON.stringify(jsonResponse));
    
    if (jsonResponse.success) {
      Logger.log('Export Job Status: ' + jsonResponse.result[0].status);
      return jsonResponse.result[0].status;
    } else {
      Logger.log('Error checking bulk export job status: ' + jsonResponse.errors[0].message);
    }
  } catch (error) {
    Logger.log('Error checking export job status: ' + error);
  }
}

// Function to Download the Bulk Extract File
function downloadBulkExtractFile(exportId) {
  try {
    var token = getAccessToken();
    var url = 'https://' + MUNCHKIN_ID + '/bulk/v1/leads/export/' + exportId + '/file.json?access_token=' + token;
    
    // Make the API request to download the file
    var response = retryableFetch(url, {
      'headers': {
        'Authorization': 'Bearer ' + token
      }
    });
    
    var fileContent = response.getContentText(); // The CSV file content
    Logger.log('Bulk Extract File Content: ' + fileContent);
    
    // Option to either save to Google Drive or Google Sheets
    saveToGoogleDrive(fileContent, 'LeadExport_' + exportId + '.csv'); // Save to Google Drive
    // saveToGoogleSheets(fileContent); // Uncomment if you'd rather save the data into Google Sheets
    
  } catch (error) {
    Logger.log('Error downloading bulk extract file: ' + error);
  }
}

// Helper function to save file to Google Drive
function saveToGoogleDrive(fileContent, fileName) {
  var folder = DriveApp.getRootFolder(); // Save to root folder of Google Drive
  var file = folder.createFile(fileName, fileContent, MimeType.CSV);
  Logger.log('File saved to Google Drive: ' + fileName);
}

// Helper function to save the data directly to Google Sheets
function saveToGoogleSheets(fileContent) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  var rows = Utilities.parseCsv(fileContent);

  sheet.clear(); // Clear any existing data
  rows.forEach(function(row) {
    sheet.appendRow(row);
  });

  Logger.log('Data written to Google Sheets successfully.');
}

// Helper function to calculate the 31-day range based on a start date
function getDateRange(startDate) {
  var start = new Date(startDate);
  var end = new Date(start);
  end.setDate(end.getDate() + 30); // Set a 31-day range

  return {
    startAt: start.toISOString(),
    endAt: end.toISOString()
  };
}

// Helper function to retry failed API requests
function retryableFetch(url, options, maxRetries = 3) {
  for (var attempt = 0; attempt < maxRetries; attempt++) {
    try {
      var response = UrlFetchApp.fetch(url, options);
      return response;
    } catch (error) {
      Logger.log('Attempt ' + (attempt + 1) + ' failed: ' + error);
      Utilities.sleep(2000); // Wait before retrying
    }
  }
  throw new Error('Failed after ' + maxRetries + ' attempts');
}

// Function to Run the Full Bulk Export Workflow
function runBulkExtract(startDate) {
  var exportId = startBulkExtract(startDate);
  
  if (exportId) {
    // Keep checking the job status until it's completed
    var status = 'Queued';
    var maxRetries = 20; // Set maximum number of retries
    var retryCount = 0;

    while ((status === 'Queued' || status === 'Processing') && retryCount < maxRetries) {
      status = checkBulkExportStatus(exportId);
      Logger.log('Current Status: ' + status);
      
      // Add a small delay between checks (e.g., wait 30 seconds)
      Utilities.sleep(30000);
      retryCount++;
    }

    if (retryCount >= maxRetries) {
      Logger.log('Max retries reached. Stopping script.');
      return;
    }
    
    // Once the status is 'Completed', download the file
    if (status === 'Completed') {
      downloadBulkExtractFile(exportId);
    } else {
      Logger.log('Bulk export job failed or was canceled.');
    }
  }
}
