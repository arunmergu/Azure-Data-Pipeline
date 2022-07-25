# Azure-Data-Pipeline


Project overview:

	This sample project showcases the end to end data engineering pipeline for the MovieLens dataset which i developed on Data-bricks workspace.
	The pipeline is setup considering the realtime workloads. For the very first time, we load the staging delta tables with full loads and run the business logic in Transform layer to generate required insights.
	From the next run onwards, we trigger the IncrementalLoad notebook which loads the incremental files and merges with the delta tables maintaining ACID principles.

Execution pipeline:
	First run : InitialLoadNotebook => Transform notebook
	Further runs: IncrementalLoadNotebook => Transform notebook

Prerequisites:

	Load the input datasets into dbfs.
	File -> Upload Data -> Select required files -> Upload to DBFS.

Notebook descriptions:

	InitialLoad:
	It loads the Movies datasets to delta tables for the first time and checks if all the tables are created properly.

	IncrementalLoad:
	Reads the Adhoc input movie datasets, checks if they have the same structure and datatypes as of the delta tables.
	Then merges the new files with the delta tables using merge into.

	Transform:
	Contains the logic for splitting of genres based on pipe separation and top N movies by average rating for the movies which have at-least 5 user reviews.
