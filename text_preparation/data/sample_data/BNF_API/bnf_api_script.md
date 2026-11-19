# BNF Data Download from internal API script

## General needs

We want the script `impresso-text-acquisition/text_preparation/importers/bnf/fetch_data_via_API.py` to implement the following functionalities:

- download the XML text data (and metadata) for all new titles of the BNF collection, which are listed in this CSV file `impresso-text-acquisition/text_preparation/data/sample_data/BNF_API/BnF_API_info/titles_to_download.csv`
- save them in our local NAS (at `/mnt/project_impresso_rw/original/BNF/`) following out usual structure
- create the corresponding issue index file following the same logic and structure as the ones already present in the `issue_indices` folder (`impresso-text-acquisition/text_preparation/data/issue_indices`)

The downloading logic will be the same for each title:

- Fetch the title's ark_id from the CSV file
- Read the corresponding `.txt` file in the folder `impresso-text-acquisition/text_preparation/data/sample_data/BNF_API/BnF_API_info/arks_num_per_ark_bib` which lists all the issue ark_ids for that title.
- Query the BNF IIIF presentation API for each issue to recover:
  - The newspaper's title (sanity check)
  - The issue's date
  - The issue's language
  - The list of the issue's pages ALTO.XML API URIs
  - The list of the issue's pages dimensions (width and height)
  -> Note that the function `get_basic_info()` in the notebook `impresso-text-acquisition/notebooks/download_bnf_data.ipynb` implements this logic given the response of the IIIF presentation API for a given issue ark_id.
  - Once the METS.XML files will be available on the presentation API, also download it for each issue, and if the information is available, store whether the issue has undergone OLR in the issue index file.
- Identify the exact canonical ID for the issue, namely ensuring that the edition letter is correctly identified when multiple issues have the same date.
  -> Note the notebook `impresso-text-acquisition/notebooks/index_generator.ipynb` solves this in several case scenarios and can be used for inspiration.
- Download the ALTO.XML files for each page of the issue (ideally named after the page's canonical ID), as well as the resulting IIIF presentation API JSON, and save them in the corresponding folder in our NAS.
- Save the relevant information for the issue_index file, including the issue's date, edition letter and local path.

In addition, the script should be robust and practical :

- Log the progress of the downloading process, and any errors encountered.
- Allow for configurations of which titles to download, and the local paths where to gather information and save the data.
- Allow for the process to be resumed, per title being probably enough for our needs, in case of interruptions or errors. (An option would be to systematically write the current list of issues to the issue_index file upon an error to directly resume from the last issue downloaded.)
- Ensure that the API access token is valid and refreshed if needed.

## Existing resources, code and notebooks

- Exploratory and debug notebook with the creation of the title-ark_id to alias mapping, and some functions fetching the info from the IIIF presentation API: `impresso-text-acquisition/notebooks/download_bnf_data.ipynb`
- CSV with the list of titles to consider for downloading.
- The folder containing the lists of issue ark_ids for each title ark_id: `impresso-text-acquisition/text_preparation/data/sample_data/BNF_API/BnF_API_info/arks_num_per_ark_bib`.
- The emails from the BNF; Ludovic being the principal contact for the API access and use.
