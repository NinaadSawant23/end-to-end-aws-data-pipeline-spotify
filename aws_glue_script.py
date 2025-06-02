import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Artists
Artists_node1746984078956 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewith-data-98/staging/artists.csv"], "recurse": True}, transformation_ctx="Artists_node1746984078956")

# Script generated for node Albums
Albums_node1746984103969 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewith-data-98/staging/albums.csv"], "recurse": True}, transformation_ctx="Albums_node1746984103969")

# Script generated for node Tracks
Tracks_node1746984104502 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewith-data-98/staging/track.csv"], "recurse": True}, transformation_ctx="Tracks_node1746984104502")

# Script generated for node Join Album-Artist
JoinAlbumArtist_node1746984456837 = Join.apply(frame1=Artists_node1746984078956, frame2=Albums_node1746984103969, keys1=["id"], keys2=["artist_id"], transformation_ctx="JoinAlbumArtist_node1746984456837")

# Script generated for node Join with Tracks
JoinwithTracks_node1746984967175 = Join.apply(frame1=Tracks_node1746984104502, frame2=JoinAlbumArtist_node1746984456837, keys1=["track_id"], keys2=["track_id"], transformation_ctx="JoinwithTracks_node1746984967175")

# Script generated for node Drop Fields
DropFields_node1746985096945 = DropFields.apply(frame=JoinwithTracks_node1746984967175, paths=["track_id", "id"], transformation_ctx="DropFields_node1746985096945")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1746985096945, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746984060153", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1746985173185 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1746985096945, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-datewith-data-98/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1746985173185")

job.commit()