package su.lasteris;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class House {
    String parent;
    String build_number;
    LocalDate end_date;
    Long est_status;
    String house_guid;
    String house_id;
    String house_number;
    String house_status;
    String ifnsfl;
    Long ifnsul;
    String okato;
    String oktmo;
    String postal_code;
    LocalDate start_date;
    String structure_number;
    Long structure_status;
    String terrifnsfl;
    String terrifnsul;
    LocalDate update_date;
    String normative_document;
    Integer counter;
    String cad_number;
    Long division_type;

    @Override
    public String toString() {
        return parent + '@' +
                (build_number == null ? "" : build_number) + '@' +
                (end_date == null ? "" : end_date.format(DateTimeFormatter.ISO_LOCAL_DATE)) + '@' +
                (est_status == null ? "" : est_status.longValue()) + '@' +
                house_guid + '@' +
                house_id + '@' +
                (house_number == null ? "" : house_number) + '@' +
                (house_status == null ? "" : house_status) + '@' +
                (ifnsfl == null ? "" : ifnsfl) + '@' +
                (ifnsul == null ? "" : ifnsul.longValue()) + '@' +
                (okato == null ? "" : okato) + '@' +
                (oktmo == null ? "" : oktmo) + '@' +
                (postal_code == null ? "" : postal_code) + '@' +
                (start_date == null ? "" : start_date.format(DateTimeFormatter.ISO_LOCAL_DATE)) + '@' +
                (structure_number == null ? "" : structure_number) + '@' +
                (structure_status == null ? "" : structure_status.longValue()) + '@' +
                (terrifnsfl == null ? "" : terrifnsfl) + '@' +
                (terrifnsul == null ? "" : terrifnsul) + '@' +
                (update_date == null ? "" : update_date.format(DateTimeFormatter.ISO_LOCAL_DATE)) + '@' +
                (normative_document == null ? "" : normative_document) + '@' +
                (counter == null ? "" : counter.intValue()) + '@' +
                (cad_number == null ? "" : cad_number) + '@' +
                (division_type == null ? "" : division_type.longValue());
    }
}
