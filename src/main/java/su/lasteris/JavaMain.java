package su.lasteris;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@QuarkusMain
public class JavaMain implements QuarkusApplication {

    @Inject
    Database db;

    final int aoguid_pos = 0;
    final int build_number_pos = 1;
    final int end_date_pos = 2;
    final int est_status_pos = 3;
    final int house_guid_pos = 4;
    final int house_id_pos = 5;
    final int house_number_pos = 6;
    final int house_status_pos = 7;
    final int ifnsfl_pos = 8;
    final int ifnsul_pos = 9;
    final int okato_pos = 10;
    final int oktmo_pos = 11;
    final int postal_code_pos = 12;
    final int start_date_pos = 13;
    final int structure_number_pos = 14;
    final int structure_status_pos = 15;
    final int terrifinsfl_pos = 16;
    final int terrifnsul_pos = 17;
    final int update_date_pos = 18;
    final int normative_doc_pos = 19;
    final int counter_pos = 20;
    final int cad_number_pos = 21;
    final int division_type_pos = 22;

    final String rpath;
    final String table;
    final String pk;
    FileReader fr;
    BufferedReader br;

    public JavaMain() throws IOException {
        rpath = ConfigProvider.getConfig().getValue("csv.path", String.class);
        table = ConfigProvider.getConfig().getValue("table.name", String.class);
        pk = ConfigProvider.getConfig().getValue("table.pk", String.class);


    }

    @Override
    public int run(String... args) throws Exception {

        db.executeUpdate(String.format("create table if not exists %s\n" +
                "(\n" +
                "id bigserial not null\n" +
                "constraint %s\n" +
                "primary key,\n" +
                "aoguid text,\n" +
                "build_number text,\n" +
                "end_date date,\n" +
                "est_status bigint,\n" +
                "house_guid text,\n" +
                "house_id text,\n" +
                "house_number text,\n" +
                "house_status bigint,\n" +
                "ifnsfl text,\n" +
                "ifnsul bigint,\n" +
                "okato text,\n" +
                "oktmo text,\n" +
                "postal_code text,\n" +
                "start_date date,\n" +
                "structure_number text,\n" +
                "structure_status bigint,\n" +
                "terrifnsfl text,\n" +
                "terrifnsul text,\n" +
                "update_date date,\n" +
                "normative_document text,\n" +
                "counter integer,\n" +
                "cad_number text,\n" +
                "division_type bigint\n" +
                ");",table, pk),
                Collections.emptyList());

        File dir1 =  new File(rpath);
        File[] files1 = dir1.listFiles((d, name) -> name.endsWith(".csv"));

        assert files1 != null;

        String n_path = rpath + File.separator + "corrected_files" + File.separator;

        Files.createDirectories(Path.of(n_path));


        System.out.println("created folder '" + n_path + "' for new files");
        System.out.println("Files rewriting process started");

        for(File file : files1) {

            String newFilename = file.getName() + "_" + "corr.csv";

            System.out.println(file.getName() + " is rewriting to " + n_path + newFilename);

           List<String> lines = Files.lines(file.toPath()).collect(Collectors.toList());

           for(int index = 0; index < lines.size(); ++index) {
               try {
                   if(index == 0)
                       Files.writeString(Path.of(n_path, newFilename), lines.get(0) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                   else {
                       String[] parts = lines.get(index).split("@", -1);

                       House house = parse(parts);

                       Files.writeString(Path.of(n_path, newFilename), house.toString() + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                   }
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
            System.out.println(file.getName() + " was rewritten.");

            if(file.delete())
                System.out.println(file.getName() + " deleted.");
        }

        File dir2 = new File(n_path);
        File[] files2 = dir2.listFiles((d, name) -> name.endsWith(".csv"));

        assert files2 != null;

        System.out.println("Files copying process started");

        for(File file : files2) {
            System.out.println("copying of " + file.getName() + "..");
            fr = new FileReader(file, StandardCharsets.UTF_8);
            br = new BufferedReader(fr);
            long rows = db.copyIn(String.format("copy %s (\n" +
                    "                 aoguid,\n" +
                    "                 build_number,\n" +
                    "                 end_date,\n" +
                    "                 est_status,\n" +
                    "                 house_guid,\n" +
                    "                 house_id,\n" +
                    "                 house_number,\n" +
                    "                 house_status,\n" +
                    "                 ifnsfl,\n" +
                    "                 ifnsul,\n" +
                    "                 okato,\n" +
                    "                 oktmo,\n" +
                    "                 postal_code,\n" +
                    "                 start_date,\n" +
                    "                 structure_number,\n" +
                    "                 structure_status,\n" +
                    "                 terrifnsfl,\n" +
                    "                 terrifnsul,\n" +
                    "                 update_date,\n" +
                    "                 normative_document,\n" +
                    "                 counter,\n" +
                    "                 cad_number,\n" +
                    "                 division_type)\n" +
                    "    from stdin with (format csv, header true, delimiter '@', null '', encoding 'utf8');", table), br);
            System.out.println(rows + "rows inserted");

            if(file.delete())
                System.out.println(file.getName() + " deleted");
        }
        return 0;
    }

    public House parse(String[] line) {
        String aoguid = line[aoguid_pos].replaceAll("\"", "");
        String build_number = line[build_number_pos].replaceAll("\"", "");
        String end_date = line[end_date_pos].replaceAll("\"", "");
        String est_status = line[est_status_pos].replaceAll("\"", "");
        String house_guid = line[house_guid_pos].replaceAll("\"", "");
        String house_id = line[house_id_pos].replaceAll("\"", "");
        String house_number = line[house_number_pos].replaceAll("\"", "");
        String house_status = line[house_status_pos].replaceAll("\"", "");
        String ifnsfl = line[ifnsfl_pos].replaceAll("\"", "");
        String ifnsul = line[ifnsul_pos].replaceAll("\"", "");
        String okato = line[okato_pos].replaceAll("\"", "");
        String oktmo = line[oktmo_pos].replaceAll("\"", "");
        String postal_code = line[postal_code_pos].replaceAll("\"", "");
        String start_date = line[start_date_pos].replaceAll("\"", "");
        String structure_number = line[structure_number_pos].replaceAll("\"", "");
        String structure_status = line[structure_status_pos].replaceAll("\"", "");
        String terrifnsfl = line[terrifinsfl_pos].replaceAll("\"", "");
        String terrifnsul = line[terrifnsul_pos].replaceAll("\"", "");
        String update_date = line[update_date_pos].replaceAll("\"", "");
        String norm_doc = line[normative_doc_pos].replaceAll("\"", "");
        String cad = line[cad_number_pos].replaceAll("\"", "");
        String counter = line[counter_pos].replaceAll("\"", "");
        String div_type = line[division_type_pos].replaceAll("\"", "");

        House house =  new House();
        house.parent = aoguid;
        house.build_number = build_number.isEmpty() ? null : build_number;
        house.end_date = end_date.isEmpty() ? null : LocalDate.parse(end_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        house.est_status = est_status.isEmpty() ? null: Long.parseLong(est_status);
        house.house_guid =  house_guid.isEmpty() ? null : house_guid;
        house.house_id = house_id.isEmpty() ? null : house_id;
        house.house_number = house_number.isEmpty() ? null : house_number;
        house.house_status = house_status.isEmpty() ? null : house_status;
        house.ifnsfl = ifnsfl.isEmpty() ? null : ifnsfl;
        house.ifnsul = ifnsul.isEmpty() ? null : Long.parseLong(ifnsul);
        house.okato = okato.isEmpty() ? null : okato;
        house.oktmo = oktmo.isEmpty() ? null : oktmo;
        house.postal_code = postal_code.isEmpty() ? null : postal_code;
        house.start_date = start_date.isEmpty() ? null : LocalDate.parse(start_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        house.structure_number = structure_number.isEmpty() ? null : structure_number;
        house.structure_status = structure_status.isEmpty() ? null : Long.parseLong(structure_status);
        house.terrifnsfl =  terrifnsfl.isEmpty() ? null : terrifnsfl;
        house.terrifnsul = terrifnsul.isEmpty() ? null : terrifnsul;
        house.update_date = update_date.isEmpty() ? null : LocalDate.parse(update_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        house.normative_document = norm_doc.isEmpty() ? null : norm_doc;
        house.counter = counter.isEmpty() ? null : Integer.parseInt(counter);
        house.cad_number = cad.isEmpty() ? null : cad;
        house.division_type = div_type.isEmpty() ? null : Long.parseLong(div_type);
        return house;
    }
}
