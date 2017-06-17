package avro.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.avro.AvroFactory
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import groovy.transform.Canonical
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ThreadLocalRandom

/**
 * Exercise the various codec scenarios.
 */
class AvroUnitTest extends Specification {

    @Canonical
    static class User100 {
        String name
    }

    @Canonical
    static class User110 {
        String name
        String username
    }

    static String randomString() {
        Integer.toHexString( ThreadLocalRandom.current().nextInt( 0, Integer.MAX_VALUE ) )
    }

    static AvroSchema loadJacksonSchema(String schemaFile ) {
        def schemaStream = AvroUnitTest.class.classLoader.getResourceAsStream( schemaFile )
        def raw = new Schema.Parser().setValidate( true ).parse( schemaStream )
        new AvroSchema( raw )
    }

    static Schema loadSchema(String schemaFile ) {
        def schemaStream = AvroUnitTest.class.classLoader.getResourceAsStream( schemaFile )
        new Schema.Parser().setValidate( true ).parse( schemaStream )
    }

    static def v100Builder = {
        new User100( name: randomString() )
    }

    static def v110Builder = {
        new User110( name: randomString(), username: randomString() )
    }

    static def v100tov100Expectation = { User100 writer, User100 reader ->
        writer.name == reader.name
    }

    static def v110tov100Expectation = { User110 writer, User100 reader ->
        writer.name == reader.name
        // reader doesn't care about username yet
    }

    static def v100tov110Expectation = { User100 writer, User110 reader ->
        (writer.name == reader.name) && ('foo' == reader.username)
    }

    @Ignore
    @Unroll( '#description' )
    void 'exercise object-based'() {

        given: 'a writer schema'
        def writerSchema = loadJacksonSchema( writerSchemaFile )

        and: 'a mapper'
        def mapper = new ObjectMapper( new AvroFactory() )

        and: 'a reader schema'
        def readerSchema = loadJacksonSchema( readerSchemaFile )

        and: 'an encoded instance'
        def original = writerClosure.call()
        byte[] encoded = mapper.writer( writerSchema ).writeValueAsBytes( original )

        when: 'the instance is decoded'
        def decoded = mapper.readerFor( readerType ).with( readerSchema ).readValue( encoded )

        then: 'encoded and decoded match'
        expectation.call( original, decoded )

        where:
        writerSchemaFile          | readerSchemaFile          | writerClosure | readerType    | description                    || expectation
        'schemas/user-1.0.0.json' | 'schemas/user-1.0.0.json' | v100Builder   | User100.class | 'Reader matches writer'        || v100tov100Expectation
        'schemas/user-1.1.0.json' | 'schemas/user-1.0.0.json' | v110Builder   | User100.class | 'Writer adds additional field' || v110tov100Expectation
        'schemas/user-1.0.0.json' | 'schemas/user-1.1.0.json' | v100Builder   | User110.class | 'Reader adds additional field' || v100tov110Expectation
    }

    static def v100MapBuilder = { Schema schema ->
        def record = new GenericData.Record( schema )
        record.put( 'name', randomString() )
        record
    }

    static def v110MapBuilder = { Schema schema ->
        def record = new GenericData.Record( schema )
        record.put( 'name', randomString() )
        record.put( 'username', randomString() )
        record
    }

    static def v100tov100RecordExpectation = { GenericRecord writer, GenericRecord reader ->
        writer.get( 'name' ) as String == reader.get( 'name' ) as String
    }

    static def v110tov100RecordExpectation = { GenericRecord writer, GenericRecord reader ->
        (writer.get( 'name' ) as String == reader.get( 'name' ) as String) &&
        writer.get( 'username' ) as String &&
        !reader.get( 'username' ) as String // reader does not have a username
    }

    static def v100tov110RecordExpectation = { GenericRecord writer, GenericRecord reader ->
        (writer.get( 'name' ) as String == reader.get( 'name' ) as String) &&
        !writer.get( 'username' ) as String && // writer does not have a username
        reader.get( 'username' ) as String
    }

    @Unroll( '#description' )
    void 'exercise map-based'() {

        given: 'a writer schema'
        def writerSchema = loadSchema( writerSchemaFile )

        and: 'a reader schema'
        def readerSchema = loadSchema( readerSchemaFile )

        and: 'an encoded instance'
        def datumWriter = new GenericDatumWriter<GenericRecord>( writerSchema )
        def dataFileWriter = new DataFileWriter<GenericRecord>( datumWriter )
        def file = new File( 'build/users.avro' )
        dataFileWriter.create( writerSchema, file )
        def written = writerClosure.call( writerSchema ) as GenericData.Record
        dataFileWriter.append( written )
        dataFileWriter.close()

        when: 'the instance is decoded'
        def datumReader = new GenericDatumReader<GenericRecord>( readerSchema )
        def dataFileReader = new DataFileReader<GenericRecord>( file, datumReader )
        List<GenericRecord> records = []
        while ( dataFileReader.hasNext() ) {
            records << dataFileReader.next()
        }
        dataFileReader.close()
        def read = records.first()

        then: 'written and red match'
        expectation.call( written, read )

        where:
        writerSchemaFile          | readerSchemaFile          | writerClosure  | description                    || expectation
        'schemas/user-1.0.0.json' | 'schemas/user-1.0.0.json' | v100MapBuilder | 'Reader matches writer'        || v100tov100RecordExpectation
        'schemas/user-1.1.0.json' | 'schemas/user-1.0.0.json' | v110MapBuilder | 'Writer adds additional field' || v110tov100RecordExpectation
        'schemas/user-1.0.0.json' | 'schemas/user-1.1.0.json' | v100MapBuilder | 'Reader adds additional field' || v100tov110RecordExpectation
    }
}