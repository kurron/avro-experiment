package avro.example

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import spock.lang.Specification

/**
 * A learning test of the Avro library.
 */
class LearningTest extends Specification {

    def 'exercise codec'() {

        given: 'a parsed schema'
        def schema = new Schema.Parser().parse( getClass().classLoader.getResourceAsStream( 'user.avsc' ) )

        and: 'some records'
        GenericRecord user1 = new GenericData.Record( schema )
        user1.put( 'name', 'Alyssa' )
        user1.put( 'favorite_number', 256)
        // Leave favorite color null

        GenericRecord user2 = new GenericData.Record( schema )
        user2.put( 'name', 'Ben' )
        user2.put( 'favorite_number', 7 )
        user2.put( 'favorite_color', 'red' )

        and: 'encoded records to disk'
        def file = new File( 'build/users.avro' )
        def datumWriter = new GenericDatumWriter<GenericRecord>( schema )
        def dataFileWriter = new DataFileWriter<GenericRecord>( datumWriter )
        dataFileWriter.create( schema, file )
        dataFileWriter.append( user1 )
        dataFileWriter.append( user2 )
        dataFileWriter.close()

        when: 'the records are decoded'
        def datumReader = new GenericDatumReader<GenericRecord>( schema )
        def dataFileReader = new DataFileReader<GenericRecord>( file, datumReader )
        GenericRecord user = null;
        while ( dataFileReader.hasNext() ) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next( user )
            println( user )
        }

        then: 'something'
    }
}
