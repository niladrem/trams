# Składowanie danych w systemach Big Data 2021
Repozytorium grupy projektowej na przedmiot *BD3* na MiNI PW

**Temat**: Zapewnienie bezpieczeństwa w przestrzeni publicznej - analiza prędkości jazdy warszawskich tramwajów.

**Flow**:
Źródło danych: https://api.um.warszawa.pl/

- Nifi (pobieranie trams co 10s) -> Kafka -> Flink (deduplikacja) -> Kafka -> Nifi -> Hive
- Nifi (pobieranie listy przystanków co kilka dni) -> Hive
- Hive -> Pyspark (średnie prędkości cząstkowe) -> Hive
                              

**Autorzy**: Anna Urbala, Bartosz Rożek
