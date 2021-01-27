from google.cloud import videointelligence_v1p3beta1 as videointelligence
from google.cloud import automl
from google.cloud import bigquery

import datetime
import base64

project_id = ''
model_id = ''

def get_prediction(content):
    prediction_client = automl.PredictionServiceClient()

    # Get the full path of the model.
    model_full_id = automl.AutoMlClient.model_path(
        project_id, "us-central1", model_id
    )
    inference_result = []
    image = automl.Image(image_bytes=content)
    payload = automl.ExamplePayload(image=image)

    # score_threshold is used to filter the result
    params = {"score_threshold": "0.5"}

    request = automl.PredictRequest(
        name=model_full_id,
        payload=payload,
        params=params
    )
    response = prediction_client.predict(request=request)
    for result in response.payload:
        #print("Predicted class name: {}".format(result.display_name))
        class_name = "{}".format(result.display_name)
        return class_name

def detect_faces(gcs_uri):
    
    client = videointelligence.VideoIntelligenceServiceClient()
    image_list = []
    # Configure the request
    config = videointelligence.FaceDetectionConfig(
        include_bounding_boxes=False, include_attributes=False
    )
    context = videointelligence.VideoContext(face_detection_config=config)

    # Start the asynchronous request
    operation = client.annotate_video(
        request={
            "features": [videointelligence.Feature.FACE_DETECTION],
            "input_uri": gcs_uri,
            "video_context": context,
        }
    )

    print("\nProcessing video for face detection annotations.")
    result = operation.result(timeout=500)

    print("\nFinished processing.\n")

    # Retrieve the first result, because a single video was processed.
    annotation_result = result.annotation_results[0]
    i=0
    for annotation_thumbnail in annotation_result.face_detection_annotations:
        #image = annotation_thumbnail.thumbnail
        image_list.append(annotation_thumbnail.thumbnail)
    for annotation in annotation_result.face_detection_annotations:
        print("Face detected:")
        for track in annotation.tracks:
            print(
                "Segment: {}s to {}s".format(
                    track.segment.start_time_offset.seconds
                    + track.segment.start_time_offset.microseconds / 1e6,
                    track.segment.end_time_offset.seconds
                    + track.segment.end_time_offset.microseconds / 1e6,
                )
            )

    return image_list

def bq_face_update(faces, filename):
    face_count = 0
    for face in faces:
        face_count += 1
    bq_client = bigquery.Client()
    p_start_time = datetime.datetime.now()
    query = """
        INSERT `<<PROJECT_NAME>>.video_mask_processing.faces_detected`
        (video_file, face_count)
        VALUES(@filename, @face_count)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("filename", "STRING", filename),
            bigquery.ScalarQueryParameter("face_count", "INTEGER", face_count)
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish
    
def bq_update_processing(filename):
    bq_client = bigquery.Client()
    p_start_time = datetime.datetime.now()
    query = """
        UPDATE `<<PROJECT_NAME>>.video_mask_processing.uploaded_videos`
        SET process_start = @process_start, process_status = @processing
        WHERE filename = @filename
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_start", "TIMESTAMP", p_start_time),
            bigquery.ScalarQueryParameter("filename", "STRING", filename),
            bigquery.ScalarQueryParameter("processing", "STRING", "PROCESSING"),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish

def bq_mask_update(mask_result,filename):
    mask_count = 0
    unmask_count = 0
    incorrect_count = 0
    for class_val in mask_result:
        if class_val == "with_mask":
            mask_count += 1
        elif class_val == "mask_weared_incorrect":
            incorrect_count += 1
        else:
            unmask_count += 1
    
    bq_client = bigquery.Client()
    p_start_time = datetime.datetime.now()
    query = """
        INSERT `<<PROJECT_NAME>>.video_mask_processing.masks_detected`
        (video_file, mask_detected_count, mask_worn_incorrectly_count, no_mask_count)
        VALUES(@filename, @mask_detected_count, @mask_worn_incorrectly_count, @no_mask_count)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("filename", "STRING", filename),
            bigquery.ScalarQueryParameter("mask_detected_count", "INTEGER", mask_count),
            bigquery.ScalarQueryParameter("mask_worn_incorrectly_count", "INTEGER", incorrect_count),
            bigquery.ScalarQueryParameter("no_mask_count", "INTEGER", unmask_count)
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish
        
def bq_finish_process(filename):
    bq_client = bigquery.Client()
    p_end_time = datetime.datetime.now()
    query = """
        UPDATE `<<PROJECT_NAME>>.video_mask_processing.uploaded_videos`
        SET process_end = @process_end, process_status = @processing
        WHERE filename = @filename
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_end", "TIMESTAMP", p_end_time),
            bigquery.ScalarQueryParameter("filename", "STRING", filename),
            bigquery.ScalarQueryParameter("processing", "STRING", "COMPLETE"),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish

def bq_start(event,context):
    bq_client = bigquery.Client()
    table_id = '<<PROJECT_NAME>>.video_mask_processing.uploaded_videos'
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))
    query = """
        INSERT `<<PROJECT_NAME>>.video_mask_processing.uploaded_videos`
        (filename, gcs_path, process_status, upload_date)
        VALUES(@filename, @gcs_path, @process_status, @upload_date)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("filename", "STRING", "{}".format(event['name'])),
            bigquery.ScalarQueryParameter("gcs_path", "STRING", "gcs://my_video_upload/{}".format(event['name'])),
            bigquery.ScalarQueryParameter("process_status", "STRING", "NOT_STARTED"),
            bigquery.ScalarQueryParameter("upload_date", "TIMESTAMP", "{}".format(event['timeCreated']))
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish

def hello_gcs(event, context):
    gcs_uri = "gs://{}/{}".format(event['bucket'],event['name'])
    filename = "{}".format(event['name'])
    mask_result = []
    bq_start(event,context)
    bq_update_processing(filename)
    faces = detect_faces(gcs_uri)
    bq_face_update(faces, filename)
    for image in faces:
        mask_result.append(get_prediction(image))
    bq_mask_update(mask_result,filename)
    bq_finish_process(filename)from google.cloud import videointelligence_v1p3beta1 as videointelligence
from google.cloud import automl
from google.cloud import bigquery

import datetime
import base64

project_id = ''
model_id = ''

def get_prediction(content):
    prediction_client = automl.PredictionServiceClient()

    # Get the full path of the model.
    model_full_id = automl.AutoMlClient.model_path(
        project_id, "us-central1", model_id
    )
    inference_result = []
    image = automl.Image(image_bytes=content)
    payload = automl.ExamplePayload(image=image)

    # score_threshold is used to filter the result
    params = {"score_threshold": "0.5"}

    request = automl.PredictRequest(
        name=model_full_id,
        payload=payload,
        params=params
    )
    response = prediction_client.predict(request=request)
    for result in response.payload:
        #print("Predicted class name: {}".format(result.display_name))
        class_name = "{}".format(result.display_name)
        return class_name

def detect_faces(gcs_uri):
    
    client = videointelligence.VideoIntelligenceServiceClient()
    image_list = []
    # Configure the request
    config = videointelligence.FaceDetectionConfig(
        include_bounding_boxes=False, include_attributes=False
    )
    context = videointelligence.VideoContext(face_detection_config=config)

    # Start the asynchronous request
    operation = client.annotate_video(
        request={
            "features": [videointelligence.Feature.FACE_DETECTION],
            "input_uri": gcs_uri,
            "video_context": context,
        }
    )

    print("\nProcessing video for face detection annotations.")
    result = operation.result(timeout=500)

    print("\nFinished processing.\n")

    # Retrieve the first result, because a single video was processed.
    annotation_result = result.annotation_results[0]
    i=0
    for annotation_thumbnail in annotation_result.face_detection_annotations:
        #image = annotation_thumbnail.thumbnail
        image_list.append(annotation_thumbnail.thumbnail)
    for annotation in annotation_result.face_detection_annotations:
        print("Face detected:")
        for track in annotation.tracks:
            print(
                "Segment: {}s to {}s".format(
                    track.segment.start_time_offset.seconds
                    + track.segment.start_time_offset.microseconds / 1e6,
                    track.segment.end_time_offset.seconds
                    + track.segment.end_time_offset.microseconds / 1e6,
                )
            )

    return image_list

def bq_face_update(faces, filename):
    face_count = 0
    for face in faces:
        face_count += 1
    bq_client = bigquery.Client()
    p_start_time = datetime.datetime.now()
    query = """
        INSERT `<<PROJECT_NAME>>.video_mask_processing.faces_detected`
        (video_file, face_count)
        VALUES(@filename, @face_count)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("filename", "STRING", filename),
            bigquery.ScalarQueryParameter("face_count", "INTEGER", face_count)
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish
    
def bq_update_processing(filename):
    bq_client = bigquery.Client()
    p_start_time = datetime.datetime.now()
    query = """
        UPDATE `<<PROJECT_NAME>>.video_mask_processing.uploaded_videos`
        SET process_start = @process_start, process_status = @processing
        WHERE filename = @filename
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_start", "TIMESTAMP", p_start_time),
            bigquery.ScalarQueryParameter("filename", "STRING", filename),
            bigquery.ScalarQueryParameter("processing", "STRING", "PROCESSING"),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish

def bq_mask_update(mask_result,filename):
    mask_count = 0
    unmask_count = 0
    incorrect_count = 0
    for class_val in mask_result:
        if class_val == "with_mask":
            mask_count += 1
        elif class_val == "mask_weared_incorrect":
            incorrect_count += 1
        else:
            unmask_count += 1
    
    bq_client = bigquery.Client()
    p_start_time = datetime.datetime.now()
    query = """
        INSERT `<<PROJECT_NAME>>.video_mask_processing.masks_detected`
        (video_file, mask_detected_count, mask_worn_incorrectly_count, no_mask_count)
        VALUES(@filename, @mask_detected_count, @mask_worn_incorrectly_count, @no_mask_count)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("filename", "STRING", filename),
            bigquery.ScalarQueryParameter("mask_detected_count", "INTEGER", mask_count),
            bigquery.ScalarQueryParameter("mask_worn_incorrectly_count", "INTEGER", incorrect_count),
            bigquery.ScalarQueryParameter("no_mask_count", "INTEGER", unmask_count)
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish
        
def bq_finish_process(filename):
    bq_client = bigquery.Client()
    p_end_time = datetime.datetime.now()
    query = """
        UPDATE `<<PROJECT_NAME>>.video_mask_processing.uploaded_videos`
        SET process_end = @process_end, process_status = @processing
        WHERE filename = @filename
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_end", "TIMESTAMP", p_end_time),
            bigquery.ScalarQueryParameter("filename", "STRING", filename),
            bigquery.ScalarQueryParameter("processing", "STRING", "COMPLETE"),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish

def bq_start(event,context):
    bq_client = bigquery.Client()
    table_id = '<<PROJECT_NAME>>.video_mask_processing.uploaded_videos'
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))
    query = """
        INSERT `<<PROJECT_NAME>>.video_mask_processing.uploaded_videos`
        (filename, gcs_path, process_status, upload_date)
        VALUES(@filename, @gcs_path, @process_status, @upload_date)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("filename", "STRING", "{}".format(event['name'])),
            bigquery.ScalarQueryParameter("gcs_path", "STRING", "gcs://mybucket_video_upload/{}".format(event['name'])),
            bigquery.ScalarQueryParameter("process_status", "STRING", "NOT_STARTED"),
            bigquery.ScalarQueryParameter("upload_date", "TIMESTAMP", "{}".format(event['timeCreated']))
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)  # Make an API request.
    query_job.result()  # Waits for statement to finish

def hello_gcs(event, context):
    gcs_uri = "gs://{}/{}".format(event['bucket'],event['name'])
    filename = "{}".format(event['name'])
    mask_result = []
    bq_start(event,context)
    bq_update_processing(filename)
    faces = detect_faces(gcs_uri)
    bq_face_update(faces, filename)
    for image in faces:
        mask_result.append(get_prediction(image))
    bq_mask_update(mask_result,filename)
    bq_finish_process(filename)
