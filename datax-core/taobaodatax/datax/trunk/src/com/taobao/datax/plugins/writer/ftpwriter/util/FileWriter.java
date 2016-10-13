package com.taobao.datax.plugins.writer.ftpwriter.util;

import com.csvreader.CsvWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * Created by zhuhq on 2016/9/21.
 */
public abstract class FileWriter implements Closeable {

    public abstract void writeOneRecord(List<String> splitedRows) throws IOException;

    public abstract void flush() throws IOException;

    public abstract void close() throws IOException;

    public static FileWriter getWriter( String fileFormat, char fieldDelimiter, Writer writer) {
        // warn: false means plain text(old way), true means strict csv format
        if (Constant.FILE_FORMAT_TEXT.equals(fileFormat) || "txt".equals(fileFormat)) {
            return new TextFileWriter(writer, fieldDelimiter);
        } else {
            return new CsvFileWriter(writer, fieldDelimiter);
        }
    }

}

class TextFileWriter extends FileWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TextFileWriter.class);

    private char fieldDelimiter;
    private Writer textWriter;

    public TextFileWriter(Writer writer, char fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
        this.textWriter = writer;
    }

    @Override
    public void writeOneRecord(List<String> splitedRows) throws IOException {
        if(splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
        }
        textWriter.write(String.format("%s%s",
                StringUtils.join(splitedRows, this.fieldDelimiter),
                IOUtils.LINE_SEPARATOR));
    }

    @Override
    public void flush() throws IOException {
        textWriter.flush();
    }

    @Override
    public void close() throws IOException {
        textWriter.close();
    }
}

class CsvFileWriter extends FileWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CsvFileWriter.class);
    private char fieldDelimiter;
    private CsvWriter csvWriter;

    public CsvFileWriter(Writer writer, char fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
        this.csvWriter = new CsvWriter(writer, this.fieldDelimiter);
        this.csvWriter.setTextQualifier('"');
        this.csvWriter.setUseTextQualifier(true);
        this.csvWriter.setRecordDelimiter(IOUtils.LINE_SEPARATOR.charAt(0));

    }
    @Override
    public void writeOneRecord(List<String> splitedRows) throws IOException {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
        }
        this.csvWriter.writeRecord((String[]) splitedRows
                .toArray(new String[0]));
    }

    @Override
    public void flush() throws IOException {
        this.csvWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.csvWriter.close();
    }
}