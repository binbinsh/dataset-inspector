class ZenodoCreator {
  const ZenodoCreator({
    required this.name,
    required this.affiliation,
    required this.orcid,
  });

  final String name;
  final String? affiliation;
  final String? orcid;
}

class ZenodoFileSummary {
  const ZenodoFileSummary({
    required this.key,
    required this.size,
    required this.checksum,
    required this.contentUrl,
  });

  final String key;
  final int size;
  final String? checksum;
  final String contentUrl;
}

class ZenodoRecordSummary {
  const ZenodoRecordSummary({
    required this.recordId,
    required this.title,
    required this.doi,
    required this.doiUrl,
    required this.publicationDate,
    required this.version,
    required this.accessRight,
    required this.recordUrl,
    required this.creators,
    required this.files,
  });

  final int recordId;
  final String title;
  final String? doi;
  final String? doiUrl;
  final String? publicationDate;
  final String? version;
  final String? accessRight;
  final String? recordUrl;
  final List<ZenodoCreator> creators;
  final List<ZenodoFileSummary> files;
}

class ZenodoZipEntrySummary {
  const ZenodoZipEntrySummary({
    required this.name,
    required this.method,
    required this.compressedSize,
    required this.uncompressedSize,
    required this.isDir,
  });

  final String name;
  final int method;
  final int compressedSize;
  final int uncompressedSize;
  final bool isDir;
}

class ZenodoTarEntrySummary {
  const ZenodoTarEntrySummary({
    required this.name,
    required this.size,
    required this.isDir,
  });

  final String name;
  final int size;
  final bool isDir;
}

class ZenodoTarEntryListResponse {
  const ZenodoTarEntryListResponse({
    required this.offset,
    required this.length,
    required this.entries,
    required this.partial,
    required this.numEntriesTotal,
  });

  final int offset;
  final int length;
  final List<ZenodoTarEntrySummary> entries;
  final bool partial;
  final int? numEntriesTotal;
}
