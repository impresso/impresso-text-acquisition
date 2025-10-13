#!/bin/bash

TARGET_DIR="/mnt/project_impresso/original/BL"  # Change this to your actual directory
OUTPUT_FILE="/home/piconti/impresso-text-acquisition/text_preparation/data/sample_data/BL/current_data.txt"      # Output file name

# Clear output file before writing new data
> "$OUTPUT_FILE"

for dir in "$TARGET_DIR"/[0-9][0-9][0-9][0-9][0-9][0-9][0-9]; do 
    if [ -d "$dir" ]; then
        echo "Processing directory: $dir" | tee -a "$OUTPUT_FILE"

        # List possible year directories for debugging
        #echo "Subdirectories found:"
        #find "$dir" -mindepth 1 -maxdepth 1 -type d -printf "%f\n"

        # Extract 4-digit year directories correctly
        years=($(find "$dir" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" | grep -E '^[0-9]{4}$' | sort -n))

        # Debug output: Print found year directories
        #echo "Detected years: ${years[*]}"

        # Collect year directories and sort them
        #years=($(find "$dir" -mindepth 1 -maxdepth 1 -type d -regex ".*/[0-9]\{4\}" | awk -F'/' '{print $NF}' | sort -n))
    
        # Check if we found any years
        if [ ${#years[@]} -gt 0 ]; then
            first_year=${years[0]}
            last_year=${years[${#years[@]}-1]}  # Correct way to get the last element
        else
            first_year="N/A"
            last_year="N/A"
        fi

        # Collect 5 random XML files
        xml_files=($(find "$dir" -type f -name "*.xml" | shuf -n 5))
        
        {
            echo "Directory: $(basename "$dir")"
            echo "First Year: $first_year"
            echo "Last Year: $last_year"
            echo "Sample XML Files:"
            if [ ${#xml_files[@]} -gt 0 ]; then
                printf "%s\n" "${xml_files[@]}"
            else
                echo "No XML files found."
            fi
            echo "-----------------------------"
        } | tee -a "$OUTPUT_FILE"
    fi
done

echo "Output written to $OUTPUT_FILE"