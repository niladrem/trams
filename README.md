# Składowanie danych w systemach Big Data 2021
Repozytorium grupy projektowej na przedmiot *BD3* na MiNI PW

**Temat**: Zapewnienie bezpieczeństwa w przestrzeni publicznej - analiza prędkości jazdy warszawskich tramwajów.

**Flow**:
Źródło danych: https://api.um.warszawa.pl/

- Nifi (pobieranie trams co 10s) -> Kafka -> Flink (deduplikacja) -> Kafka -> Nifi -> Hive
- Nifi (pobieranie listy przystanków) -> Hive
- Hive -> Pyspark (średnie prędkości cząstkowe) -> HBase
                              
**Deduplikacja**: https://github.com/niladrem/trams_flink

## Struktura repo

### docs/
Zawiera dokumentację i dokumentację z testów.

### extra/
Zawiera wszelkie skrypty pomocnicze m.in.: skrypty wywoływane przez Nifi, definicje tabel.

### nifi/
Zawiera template z flow.

### pyspark/
Zawiera skrypty pysparkowe oraz notebook z wizualizacjami.

**Autorzy**: Anna Urbala, Bartosz Rożek
