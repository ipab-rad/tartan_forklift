# tartan_dataset_management
Collection of tools to manage ROSbag recordings data from AV vehicle

# metadata generator usage


1. Setup
 Ensure all dependencies are installed. You can use the following command to install required packages:

 ```bash
    pip install mcap
```

2. Running the Script
To generate the metadata JSON file, follow these steps:

Place all your MCAP files in a directory.

Update the create_resources_json function call with the path to your directory:

 ```bash
    create_resources_json('path/to/your/dataset/directory')
```
Run the script:

 ```bash
    python metadata_generator.py
```


3. Output
The script will generate a resources.json file in the specified directory. This JSON file will contain metadata for each MCAP file in the directory.
