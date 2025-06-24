# Use the official Playwright image which comes with browsers and dependencies pre-installed
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Set the working directory inside the container
WORKDIR /app

# Copy your requirements.txt file into the container
COPY requirements.txt .

# Install your Python packages
RUN pip install -r requirements.txt

# Copy the rest of your application code into the container
COPY . .

# Specify the command to run your scraper
# The -u flag ensures that logs are not buffered and are sent straight to the console
CMD ["python", "-u", "scrape_emprego_mz_ai_powered.py"] 
