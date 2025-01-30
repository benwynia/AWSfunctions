import boto3

def get_secret(secret_name, region_name="us-east-2"):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {str(e)}")
        return None  # Handle errors or re-raise as needed

    # Decrypts secret using the associated KMS CMK
    # Depending on whether the secret is a string or binary, one of these fields will be populated
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)  # Assuming the secret is stored as a JSON string
    else:
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return decoded_binary_secret.decode('utf-8')

def load_s3_data_to_dataframes(bucket_name):
    s3_client = boto3.client('s3')
    date_today = datetime.now().strftime("%Y-%m-%d")
    folder_path = f"{date_today}/"

    # List files in the specified S3 folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    dataframes = {}

    if 'Contents' in response:
        for item in response['Contents']:
            file_name = item['Key']
            if file_name.endswith('.csv'):  # Check for .csv files
                # Remove the folder path and date from the file name
                base_name = file_name.split('/')[-1][:-13]  # This takes the file name after the last '/', and removes the last 15 characters ('_YYYYMMDD.csv')
                
                # Get the file object using boto3
                obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
                
                # Read the file object into a pandas DataFrame
                df = pd.read_csv(io.BytesIO(obj['Body'].read()))

                # Using the refined base_name as the key to store DataFrame in dictionary
                dataframes[base_name] = df
                
                print(f"Loaded {file_name} into DataFrame named '{base_name}'.")

    return dataframes

def sendEmail(subject, bodyText, bodyHtml, sender, recipient, attachmentPath=None, awsRegion="us-east-2", configurationSet="SarahAIConfigSet"):
    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=awsRegion)

    # Create a multipart/mixed parent container.
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient

    # Create a multipart/alternative child container.
    msgBody = MIMEMultipart('alternative')
    textPart = MIMEText(bodyText.encode('utf-8'), 'plain', 'utf-8')
    htmlPart = MIMEText(bodyHtml.encode('utf-8'), 'html', 'utf-8')

    # Attach parts into message container.
    msgBody.attach(textPart)
    msgBody.attach(htmlPart)
    msg.attach(msgBody)

    # Attach the file
    if attachmentPath:
        try:
            with open(attachmentPath, 'rb') as file:
                att = MIMEApplication(file.read())
                att.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachmentPath))
                msg.attach(att)
        except:
            print('Failed to open file and/or attach to email')

    try:
        # Provide the contents of the email.
        response = client.send_raw_email(
            Source=sender,
            Destinations=[recipient],
            RawMessage={'Data': msg.as_string()},
            ConfigurationSetName=configurationSet
        )
        return {
            'statusCode': 200,
            'body': "Email sent! Message ID: " + response['MessageId']
        }
    except ClientError as e:
        return {
            'statusCode': 400,
            'body': "Failed to send email: " + e.response['Error']['Message']
        }
