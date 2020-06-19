# Odoo Importer 

Automated Data Pipeline for importing employee information into Odoo 

## Environment Variables 

```AZURE_CONNECTION_STRING```: Connection string for the azure storage account 

```ODOO_URL```: The Odoo endpoint from which to run the import against not including the trailing slash

```ODOO_USER```: The Odoo user which will be used to carry out the import 

```ODOO_PASSWORD```: The password for the user specified which will be used to carry out the import 

```EMPLOYEE_MASTER_SHEET_CONTAINER```: The Blob container in the azure storage account which is holding the employee master sheet

```EMPLOYEE_MASTER_SHEET_FILE_NAME```: The file name of the employee master sheet in the Blob container specified by EMPLOYEE_MASTER_SHEET_CONTAINER 

```UPDATE_SHEET_CONTAINER```: The Blob container in the azure storage account which is holding the update sheet 

```UPDATE_SHEET_FILE_NAME```: The file name of the update sheet in the Blob container specified by UPDATE_SHEET_CONTAINER

```ORG_SHEET_CONTAINER```: The Blob container in the azure storage account which is holding the raw org sheet 

```ORG_SHEET_FILE_NAME```: The file name of the raw org sheet in the Blob container specified by ORG_SHEET_CONTAINER

```SLACK_URL```: (Optional) The webhook url to post to a slack bot for critical log messages 

## Running Import 

1. Simply set the environment variables specified above 

2. Run ```docker-compose up --build```

## License

Unless otherwise noted, the source code of this project is covered under Crown Copyright, Government of Canada, and is distributed under the [MIT License](LICENSE).
