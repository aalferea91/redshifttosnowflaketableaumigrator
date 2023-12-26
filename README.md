# Redshift to Snowflake Tableau migrator

In this project I recycled a python script given by another team from my company in order to speed up the process of migrating the current dashboards and datasources in our client's Tableau Server from Redshift datasources to Snowflake:

https://github.com/aalferea91/redshifttosnowflaketableaumigrator/blob/main/tableau_snowflake_migrator_datasource.py

https://github.com/aalferea91/redshifttosnowflaketableaumigrator/blob/main/tableau_snowflake_migrator_original.py

The process of changing each data source from Redshift to Snowflake is time consuming. Additionally when you do this manually you will encounter different types of issues due to Snowflake column uppercase naming logic. Due to this, additional actions will need to be taken like:

-Fixing missing fields by replacing them with the new name for that column given by Snowflake.

-Hierachies will need to be reconfigured.

-Sets based on conditions will need to be reconfigured.

-Extract filters will need to be reconfigured.

The python script in this repository will do the manual work automatically by editing the XML file of the Tableau workbook. In my experience most of the times the issues listed previously get fixed but in some cases there are still some issues that appear, therefore I suggest that after editing the script with your own parameters and executing it you should also look for all these previous issues to ensure that all aspects of the workbook have been fixed.
