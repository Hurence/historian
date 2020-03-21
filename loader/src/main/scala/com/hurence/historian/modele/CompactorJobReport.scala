package com.hurence.historian.modele

case class CompactorJobReport(jobId: String,
                              jobType: String,
                              start: Long,
                              end: Long,
                              status: String,
                              elapsed: Long,
                              number_of_chunk_input: Long,
                              number_of_chunk_output: Long,
                              input_chunk_ids: List[String],
                              output_chunk_ids: List[String])


object CompactorJobReport {
  val JOB_ID="id"
  val JOB_TYPE="type"
  val JOB_START="start"
  val JOB_END="end"
  val JOB_STATUS="status"
  val JOB_ERROR="error"
  val JOB_ELAPSED="job_duration_in_milli"
  val JOB_NUMBER_OF_CHUNK_INPUT="number_of_chunks_in_input"
  val JOB_NUMBER_OF_CHUNK_OUTPUT="number_of_chunks_in_output"
  val JOB_INPUT_CHUNK_IDS="input_chunks_ids"
  val JOB_OUTPUT_CHUNK_IDS="output_chunks_ids"
  val JOB_TOTAL_METRICS_RECHUNKED="total_metrics_rechunked"
  val JOB_CONF="job_conf"

  val JOB_TYPE_VALUE="compaction"
  val DEFAULT_COLLECTION="historian-reports"
}