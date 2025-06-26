import json
import webbrowser
import os

def run_manual_entry(source_file, output_file):
    """
    Opens job URLs for manual data entry and saves the results.
    """
    # Load the failed job entries
    try:
        with open(source_file, 'r', encoding='utf-8') as f:
            jobs_to_process = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Error: Could not read or decode {source_file}. Please ensure it contains the failed jobs.")
        return

    if not jobs_to_process:
        print("No jobs to process.")
        return

    print("--- Starting Manual Data Entry ---")
    print("For each job, a webpage will open. Please copy the requested information from the page into the terminal.")
    
    completed_jobs = []

    for i, job in enumerate(jobs_to_process):
        url = job.get("source_url")
        if not url:
            continue

        print(f"\n--- Processing Job {i+1}/{len(jobs_to_process)} ---")
        print(f"Opening URL: {url}")
        webbrowser.open(url)

        try:
            job_title = input("Enter Job Title: ")
            company_name = input("Enter Company Name: ")
            location = input("Enter Location (City): ")
            publication_date = input("Enter Publication Date (YYYY-MM-DD or text): ")
            expiring_date = input("Enter Expiring Date (YYYY-MM-DD or text): ")
            job_description = input("Enter Job Description (a brief summary): ")
            tasks_of_the_role = input("Enter Tasks/Responsibilities (use '\\n' for new lines): ")
            requirements = input("Enter Requirements (use '\\n' for new lines): ")
            category = input("Enter Category: ")

            # Create the new, complete job object
            new_job_data = {
                "job_title": job_title,
                "company_name": company_name,
                "location": location,
                "publication_date": publication_date,
                "expiring_date": expiring_date,
                "job_description": job_description,
                "tasks_of_the_role": tasks_of_the_role.replace("\\n", "\n"),
                "requirements": requirements.replace("\\n", "\n"),
                "category": category,
                "source_url": url,
            }
            completed_jobs.append(new_job_data)

        except KeyboardInterrupt:
            print("\nManual entry interrupted. Saving progress...")
            break
        except Exception as e:
            print(f"An error occurred: {e}. Skipping this job.")
            continue

    if not completed_jobs:
        print("\nNo jobs were processed.")
        return

    # Save the manually entered data
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(completed_jobs, f, ensure_ascii=False, indent=2)
        print(f"\nSuccessfully saved {len(completed_jobs)} manually entered jobs to {output_file}.")
    except Exception as e:
        print(f"\nError saving to {output_file}: {e}")

if __name__ == '__main__':
    run_manual_entry('un_jobs_mz.json', 'un_jobs_mz_manual.json') 