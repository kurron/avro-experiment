package org.kurron.avro.example

import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter

/**
 * Should run before the tests, outputting this schema's data to a file.
 */
class DatFileWriter {

    static final dataFileLocation = '../v110.bin'
    public static final String NAME = 'name-v110'
    public static final String USERNAME = 'username-v110'

    static void main(String[] args) {
        def encoded = User.newBuilder().setName(NAME).setUsername(USERNAME).build()
        def datumWriter = new SpecificDatumWriter<User>( User )
        def dataFileWriter = new DataFileWriter<User>( datumWriter )
        dataFileWriter.create( encoded.getSchema(), new File( dataFileLocation ) )
        dataFileWriter.append( encoded )
        dataFileWriter.flush()
        dataFileWriter.close()
    }
}
