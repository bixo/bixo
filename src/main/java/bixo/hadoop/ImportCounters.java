package bixo.hadoop;

public enum ImportCounters {
    URLS_FILTERED,      // URLs removed due to max URL constraint
    URLS_REJECTED,      // URLS rejected because they were invalid
    URLS_ACCEPTED,      // URLs accepted
}
