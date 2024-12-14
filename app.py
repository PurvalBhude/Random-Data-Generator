import os
import json
import random
import time
import zipfile
import traceback
from flask import Flask, request, render_template, jsonify, send_file, send_from_directory

app = Flask(__name__)

def extract_schema_from_json(json_data):
    """
    Extract schema from complex JSON metadata file.
    
    Args:
        json_data (dict): Parsed JSON data containing element metadata
    
    Returns:
        tuple: (table_name, schema_dict)
    """
    # Determine table name (prefer schemaKey, then name, fallback to 'unknown_table')
    table_name = json_data.get('schemaKey', json_data.get('name', 'unknown_table'))
    entity_key = json_data.get('entityKey', 'default_entity')
    
    # Create schema dictionary
    schema = {table_name: {}}
    
    # Map JSON datatype to our generation types
    datatype_mapping = {
        'STRING': 'string',
        'INTEGER': 'integer',
        'INT': 'integer',
        'TIMESTAMP': 'timestamp',
        'DATETIME': 'timestamp'
    }
    
    # Process attributes if they exist
    if 'attributes' in json_data and isinstance(json_data['attributes'], list):
        for attr in json_data['attributes']:
            # Get attribute name and datatype
            attr_name = attr.get('name', '')
            # Try multiple paths to get datatype, default to string if not found
            datatype = datatype_mapping.get(
                attr.get('datatype', '').upper(), 
                datatype_mapping.get(attr.get('logicalDatatype', '').upper(), 'string')
            )
            
            # Add to schema if attribute name is not empty
            if attr_name:
                schema[table_name][attr_name] = datatype
    
    # If no attributes found, add a default key
    if not schema[table_name]:
        schema[table_name]['key'] = 'string'
    
    # Add operation attribute
    schema[table_name]['operation'] = 'operation'
    
    return table_name, entity_key, schema

def generate_random_transaction(schema, count, base_dir="data", schema_key=None, entity_key=None):
    """Generate random data based on the given schema and save it into separate folders."""
    # Ensure the base directory exists
    os.makedirs(base_dir, exist_ok=True)

    generated_files = []

    for table_name, attributes in schema.items():
        table_dir = os.path.join(base_dir, table_name)
        os.makedirs(table_dir, exist_ok=True)  # Create a folder for each table

        for i in range(count):
            data = {
                "startTransaction": True,
                "transactionId": f"transaction{random.randint(1, 100)}",
                "endTransaction": True,
                "repeatedMessages": {table_name: []}
            }

            record = {}
            for attribute_name, attribute_type in attributes.items():
                if attribute_type == "string":
                    if attribute_name.lower() in ["sys_creation_date", "key", "createdby"]:
                        record[attribute_name] = f"V{random.randint(1, 100)}"
                    else:
                        record[attribute_name] = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=8))
                elif attribute_type == "integer":
                    record[attribute_name] = str(i + 1) if attribute_name.lower() == "customer_id" else str(random.randint(1, 100))
                elif attribute_type == "timestamp":
                    record[attribute_name] = int(time.time() * 1000)
                elif attribute_type == "operation":
                    record[attribute_name] = {
                        "enumName": "Operation",
                        "valueName": "UPSERT",
                        "valueOrdinal": 1
                    }
                else:
                    record[attribute_name] = None

            data["repeatedMessages"][table_name].append(record)

            # Generate filename with schemaKey_entityKey_fileNo format
            filename = f"{schema_key}_{entity_key}_file{i+1}.json"
            file_path = os.path.join(table_dir, filename)
            
            # Save each record as a JSON file
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=4)
            
            generated_files.append(file_path)

    return generated_files

def create_zip_archive(files, archive_name='generated_data.zip'):
    """
    Create a zip archive of the generated files.
    
    Args:
        files (list): List of file paths to include in the zip
        archive_name (str): Name of the zip archive
    
    Returns:
        str: Path to the created zip archive
    """
    # Ensure downloads directory exists
    downloads_dir = 'downloads'
    os.makedirs(downloads_dir, exist_ok=True)
    
    # Create zip file path
    zip_path = os.path.join(downloads_dir, archive_name)
    
    # Create zip archive
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in files:
            # Add file to zip, preserving directory structure relative to 'data'
            arcname = os.path.relpath(file, 'data')
            zipf.write(file, arcname=arcname)
    
    return zip_path

@app.route('/generate', methods=['POST'])
def generate():
    if 'schema_file' not in request.files:
        return jsonify({"error": "No file uploaded"})
    
    schema_file = request.files['schema_file']
    
    if schema_file.filename == '':
        return jsonify({"error": "No selected file"})

    count = int(request.form['count'])

    temp_dir = 'tmp_schema_files'  # Use tmp directory for temporary storage
    os.makedirs(temp_dir, exist_ok=True)

    output_dir = '/tmp/data'  # Vercel provides /tmp for temporary files
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Save uploaded file
        schema_file_path = os.path.join(temp_dir, schema_file.filename)
        schema_file.save(schema_file_path)

        all_generated_files = []

        if schema_file.filename.lower().endswith('.zip'):
            with zipfile.ZipFile(schema_file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file.lower().endswith('.json'):
                        file_path = os.path.join(root, file)
                        
                        try:
                            with open(file_path, 'r') as f:
                                schema_json = json.load(f)
                            
                            table_name, entity_key, schema = extract_schema_from_json(schema_json)
                            schema_key = schema_json.get('schemaKey', 'unknown_schema')
                            generated_files = generate_random_transaction(
                                schema, 
                                count, 
                                base_dir=output_dir, 
                                schema_key=schema_key, 
                                entity_key=entity_key
                            )
                            all_generated_files.extend(generated_files)
                        
                        except Exception as e:
                            print(f"Error processing {file}: {str(e)}")
                            print(traceback.format_exc())
        
        else:
            with open(schema_file_path, 'r') as f:
                schema_json = json.load(f)
            
            table_name, entity_key, schema = extract_schema_from_json(schema_json)
            schema_key = schema_json.get('schemaKey', 'unknown_schema')
            generated_files = generate_random_transaction(
                schema, 
                count, 
                base_dir=output_dir, 
                schema_key=schema_key, 
                entity_key=entity_key
            )
            all_generated_files.extend(generated_files)

        zip_path = create_zip_archive(all_generated_files)

        shutil.rmtree(temp_dir)

        return jsonify({
            "status": "success", 
            "message": f"Generated {len(all_generated_files)} files",
            "download_link": f"/download/{os.path.basename(zip_path)}"
        })

    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON format in schema file."})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)})

@app.route('/download/<filename>')
def download_file(filename):
    try:
        return send_from_directory('/tmp/downloads', filename, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "File not found"}), 404

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)