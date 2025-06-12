# Data Validator Documentation
This repository  provides an overview of how to use this code. This  repository was created based on my experience working on a data migration project from AWS Athena to Alibaba Maxcompute. During this migration, i encountered frequent cases where data IDs were missing in either AWS or Alibaba, or there were discrepancies in values. To address these issues, I developed this tool to validate data and ensure consistency between AWS and Alibaba. Not only that, I also added Oracle and Postgres database options.

## Project Overview

This tool has the ability to:
- Validate and identify missing IDs between each database AWS, Alibaba, Postgres, and oracle
- Detect discrepancies values based on unique id or composite id 

## Features 
- Missing ID Detection: finds any Ids that are missing between the databases and save into csv file 
- Value Discrepancy Detection: Compares Values of records with the same ID across different databases and save output into csv file 
- Multi Database Support: initially built for AWS and Alibaba, but now supports AWS athena, Alibaba Maxcompute, Oracle, ad postgreSQL databases

## Getting Started

Follow these steps to set up and use the Data Validator tool locally.

### Prerequisites 

Before you begin, ensure you have the following this:
- Python 
- Access to the Databases (AWS, Alibaba, Oracle, PostgreSQL)
