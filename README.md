# polka
Polka is a time series database written in Java. It stores multiple time series, a 64-bit timestamp with fixed length record. Records are only appendable. It's essentially [anabel](www.github.com/piotrmaslanka/anabel), but with a network interface.

Polka additionally supports auto-trim. This is automatic pruning of records older than serie's leading record with. This means that you can keep, for example, records from only last week. Older records will be automatically deleted.

The database is written in Java with a Python interface.

Polka is a stripped-down version of my diploma thesis, that concerned distributed time series database. This fact is terribly obvious in the repository history. For licensing, see the attached file.