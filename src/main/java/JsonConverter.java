import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;

public class JsonConverter {


    public static void main( String args[] ) throws IOException, JSONException {
        BufferedReader br = new BufferedReader(new FileReader(
                "directory of the json file"));

        BufferedWriter bw = new BufferedWriter(new FileWriter(
                "directory of the new json file you want to save"));

        String line = br.readLine();

        while ((line != null)) {
            JSONObject dataJson = new JSONObject(line);
            try {

                String reviewTime = dataJson.getString("reviewTime");
                String[] tmp = reviewTime.split(" ");
                tmp[1] = tmp[1].substring(0, tmp[1].length() - 1);
                if (tmp[1].length() < 2) {
                    tmp[1] = '0' + tmp[1];
                }
                StringBuilder bd = new StringBuilder("");
                bd.append(tmp[2]).append(tmp[0]).append(tmp[1]);
                int newTime = Integer.valueOf(bd.toString());
                dataJson.put("reviewTime", newTime);

                bw.write(dataJson + "\n");
                line = br.readLine();
                bw.flush();

            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
            }

        }

        br.close();
        bw.close();
    }

}
