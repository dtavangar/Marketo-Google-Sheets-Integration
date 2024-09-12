/**
 * Marketo REST API to Google Sheets Integration
 * 
 * Description:
 * This script fetches lead data from Marketo using the standard REST API and inserts it into Google Sheets.
 * It handles pagination automatically to ensure all leads are retrieved, even when the data spans multiple pages.
 * Lead data is filtered by email in this example, but it can be easily modified to use other filters.
 * 
 * Author: Damon Tavangar
 * Email: tavangar2017@gmail.com
 * 
 * Version: 1.0
 * License: GPL License
 */

// Marketo API Credentials
var CLIENT_ID = '<YOUR CLIENT ID>';
var CLIENT_SECRET = '<YOUR CLIENT SECRET>';
var MUNCHKIN_ID = '<YOUR MUNCHKIN_ID>';

// Function to Get Access Token
function getAccessToken() {
  var url = 'https://' + MUNCHKIN_ID + '.mktorest.com/identity/oauth/token?grant_type=client_credentials&client_id=' + CLIENT_ID + '&client_secret=' + CLIENT_SECRET;
  var response = UrlFetchApp.fetch(url, { 'method': 'post' });
  var result = JSON.parse(response.getContentText());
  return result.access_token;
}

// Function to Fetch Leads from Marketo and Insert into Google Sheets
function fetchLeads() {
  try {
    var token = getAccessToken();
    var filterType = 'email'; // Filter by email, but can be changed to other supported filters
    var filterValues = 'john.doe@example.com,jane.smith@example.com';
    var baseUrl = 'https://' + MUNCHKIN_ID + '.mktorest.com/rest/v1/leads.json?filterType=' + filterType + '&filterValues=' + filterValues + '&access_token=' + token;
    var url = baseUrl;

    var leads = [];
    var jsonResponse;
    
    // Fetch leads with pagination support
    do {
      // Fetch data from the API
      var response = UrlFetchApp.fetch(url, {
        'headers': {
          'Authorization': 'Bearer ' + token
        }
      });

      jsonResponse = JSON.parse(response.getContentText());

      // Log the raw response for debugging
      Logger.log('API Response: ' + JSON.stringify(jsonResponse));

      if (!jsonResponse.success) {
        Logger.log('Error in response: ' + jsonResponse.errors[0].message);
        return;
      }

      // Append the leads from this page to the overall list
      leads = leads.concat(jsonResponse.result);

      // Check for nextPageToken to fetch the next page
      if (jsonResponse.nextPageToken) {
        url = baseUrl + '&nextPageToken=' + jsonResponse.nextPageToken;
      } else {
        url = null; // End the loop when there's no more data
      }

    } while (url);

    // Log the leads data for debugging
    Logger.log('Leads Data: ' + JSON.stringify(leads));

    // Open the Google Sheet and paste the data if there are leads
    if (leads && leads.length > 0) {
      var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
      sheet.clear(); // Clear any previous data

      // Set headers in the first row
      var headers = Object.keys(leads[0]);
      sheet.appendRow(headers);

      // Append lead data to the sheet
      leads.forEach(function(lead) {
        var row = [];
        headers.forEach(function(header) {
          row.push(lead[header]);
        });
        sheet.appendRow(row);
      });

      Logger.log('Leads successfully written to Google Sheets.');
    } else {
      Logger.log('No leads found in the API response.');
    }

  } catch (error) {
    Logger.log('Error fetching leads: ' + error);
  }
}

// Optional Helper Function to Retry on Network Issues
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
