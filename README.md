# SF_DW

A ROLAP data warehouse in Python+Postgresql made for a university project (Advanced Data Bases class).
Datasource is taken from "Fire Department Calls For Service" dataset of the city of San Francisco (https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3)

The python script automatize the ETL procedures, materialize cleaned and transformed data to CSV files and then load them in a postgresql database. A basic query test suite is automatically executed and its time results are exported to CSV files as well.

Project developed by:
Crispino Cicala - https://github.com/p-noc
Emanule Cioffi - https://github.com/ro-nin
