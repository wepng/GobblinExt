package xiaomei.gobblin.core.util;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Created by Lambor Ryan on 16/3/11.
 * Email: ruanchengfeng@xiaomei.com
 */
public class Decompress {
    /***
     * gzip解压
     * @param bytes
     * @return
     * @throws IOException
     */
    public static byte[] decompressByGzip(byte[] bytes) throws IOException {
        //value为Array[Byte]，格式为: timestamp, thread, level, module, message
        byte[] unzipByte = null;

        if((bytes[0] == (byte)0x1f || bytes[1] == (byte)0x1f)
                && (bytes[1] == (byte)0x8b || bytes[2] == (byte)0x8b)){
            InputStream zipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes));

            unzipByte = IOUtils.toString(zipInputStream, "UTF-8").getBytes();
        }
        else {
            unzipByte = new String(bytes, "UTF-8").getBytes();
        }

        return unzipByte;
    }
}
