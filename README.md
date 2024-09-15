# Marketo Data Export and Automation Scripts

## Overview

This repository contains Google Apps Script files that integrate with Marketo's REST API and Bulk Extract API to automate lead data extraction into Google Sheets or Google Drive. The scripts enable efficient management of Marketo lead data through automated and incremental exports. Additionally, time-based triggers automate job creation and status checks, ensuring continuous data synchronization without manual intervention.

## Author

**Name**: Damon Tavangar  
**Email**: tavangar2017@gmail.com  

## License

This project is licensed under the **GPLv3 License**.

---

## Features

1. **Standard Marketo REST API Export**
   - Fetches lead data using filters (e.g., email) from Marketo's REST API and exports it to Google Sheets.
   - Automatically handles pagination to support large datasets.
   - Logs all operations and errors for debugging.

2. **Marketo Bulk Extract for a 31-Day Window**
   - Uses Marketo's Bulk Extract API to export lead data within a user-defined 31-day time frame.
   - Option to save exported data to either Google Drive or directly into Google Sheets.
   - Includes error handling, retry logic, and progress monitoring.

3. **Marketo Bulk Export with UID Tracking**
   - Automatically exports lead data while tracking the highest Marketo Lead UID to prevent duplicate downloads.
   - Ensures incremental data export by downloading only new records in each run.
   - Saves the results into Google Sheets.

4. **Automation via Time-Based Triggers**
   - Automates the creation of export jobs and status checks using Google Apps Script time-based triggers.
   - Schedules the export job every 1 minute and checks the status every 10 minutes to ensure timely data download and processing.

---

## Prerequisite

- Marketo API credentials:
  - **Client ID**
  - **Client Secret**
  - **Munchkin ID**
- Access to a Google Apps Script-enabled Google account.
- Create a Google Sheet to store the lead data, if applicable.

---

## Version

**Version**: 1.2  
**Status**: Beta  

---

## Requirements

- Google Apps Script
- Marketo REST and Bulk Extract API credentials
- A Google Sheet or Google Drive folder to store the exported data.

---

## Setup

### 1. Clone the repository

Download or clone this repository to your local machine.

### 2. Configure Google Apps Script

- Open a new Google Apps Script project from Google Drive.
- Copy the contents of each `.gs` file into separate script files within your project.
- Update the **Marketo API credentials** (`CLIENT_ID`, `CLIENT_SECRET`, `MUNCHKIN_ID`) in each script where applicable.

### 3. Set Up Script Properties

You can set up environment variables (API credentials) using Google Apps Script's `PropertiesService` for more secure management.

### 4. Set Up Time-Based Triggers

To automate data export and status checks, run the functions `createOneMinuteTriggerForJobCreation()` and `createTenMinuteTriggerForStatusCheck()` to schedule these operations.

---

## Usage

### 1. **Standard Marketo REST API Export**
Run the function `fetchLeads()` to fetch lead data from Marketo using filters (such as email) and export it to Google Sheets.

### 2. **Marketo Bulk Extract for a 31-Day Window**
Run the function `runBulkExtract(startDate)` where `startDate` is the beginning of the 31-day time frame (format: `'YYYY-MM-DD'`). The script exports data for the specified window and saves it to either Google Drive or Google Sheets.

### 3. **Marketo Bulk Export with UID Tracking**
Run the function `runBulkExportInChunks()` to export lead data incrementally. The script will track the highest UID and ensure no duplicate data is downloaded.

### 4. **Automated Job Creation and Status Checks**
To automate the export process:
- Use `createOneMinuteTriggerForJobCreation()` to create a trigger that runs every 1 minute and schedules export jobs.
- Use `createTenMinuteTriggerForStatusCheck()` to create a trigger that checks the status of export jobs every 10 minutes.

---

## Logging and Monitoring

All scripts utilize `Logger.log()` to record detailed information about their execution. Monitor these logs in the Google Apps Script editor to verify the progress of data exports and check for any errors.

---

### Example Code Execution:

```javascript
// Run the bulk extract for a specific 31-day window
runBulkExtract('2024-01-01');

// Automate the job creation and status checks
createOneMinuteTriggerForJobCreation();
createTenMinuteTriggerForStatusCheck();
