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
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.kurron.avro.example.v100.user as User100
import org.kurron.avro.example.v110.user as User110
import org.kurron.avro.example.v120.user as User120
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ThreadLocalRandom

/**
 * Exercise the various codec scenarios.
 */
class AvroUnitTest extends Specification {

    static String randomString() {
        Integer.toHexString( ThreadLocalRandom.current().nextInt( 0, Integer.MAX_VALUE ) )
    }

    static Schema loadSchema(String schemaFile ) {
        def schemaStream = AvroUnitTest.class.classLoader.getResourceAsStream( schemaFile )
        new Schema.Parser().setValidate( true ).parse( schemaStream )
    }

    static final String dataFileLocation = 'build/written.bin'

    static def v100Writer = {
        DatumWriter<User100> datumWriter = new SpecificDatumWriter<User100>( User100 )
        DataFileWriter<User100> dataFileWriter = new DataFileWriter<User100>( datumWriter )

        def user = User100.newBuilder().setName( randomString() ).build()
        dataFileWriter.create( user.getSchema(), new File( dataFileLocation ) )
        dataFileWriter.append( user )
        dataFileWriter.flush()
        dataFileWriter.close()
        user
    }

    static def v100Reader = {
        DatumReader<User100> userDatumReader = new SpecificDatumReader<User100>(User100)
        DataFileReader<User100> dataFileReader = new DataFileReader<User100>(new File(dataFileLocation), userDatumReader)
        dataFileReader.hasNext() ? dataFileReader.next( new User100() ) : new User100()
    }

    static def v110Writer = {
        DatumWriter<User110> datumWriter = new SpecificDatumWriter<User110>( User110 )
        DataFileWriter<User110> dataFileWriter = new DataFileWriter<User110>( datumWriter )

        def user = User110.newBuilder().setName( randomString() ).setUsername( randomString() ).build()
        dataFileWriter.create( user.getSchema(), new File( dataFileLocation ) )
        dataFileWriter.append( user )
        dataFileWriter.flush()
        dataFileWriter.close()
        user
    }

    static def v110Reader = {
        DatumReader<User110> userDatumReader = new SpecificDatumReader<User110>(User110)
        DataFileReader<User110> dataFileReader = new DataFileReader<User110>(new File(dataFileLocation), userDatumReader)
        dataFileReader.hasNext() ? dataFileReader.next( new User110() ) : new User110()
    }

    static def v120Reader = {
        DatumReader<User120> userDatumReader = new SpecificDatumReader<User120>(User120)
        DataFileReader<User120> dataFileReader = new DataFileReader<User120>(new File(dataFileLocation), userDatumReader)
        dataFileReader.hasNext() ? dataFileReader.next( new User120() ) : new User120()
    }

    static def v100tov100Expectation = { User100 writer, User100 reader ->
        writer == reader // should be 100% equivalent
    }

    static def v110tov100Expectation = { User110 writer, User100 reader ->
        writer.name as String == reader.name as String
        // reader doesn't care about username yet
    }

    static def v100tov110Expectation = { User100 writer, User110 reader ->
        (writer.name as String == reader.name as String) &&
        (reader.username as String == 'unknown') // writer does not know about username
    }

    static def v110tov120Expectation = { User110 writer, User120 reader ->
        (writer.name as String == reader.firstname as String) &&
        (reader.lastname as String == 'unknown') &&
        (writer.username as String == reader.username as String)
    }

    @Unroll( 'Object-based: #description' )
    void 'exercise object-based'() {

        given: 'an encoded instance'
        def written = writerClosure.call()

        when: 'the instance is decoded'
        def read = readerClosure.call()

        then: 'encoded and decoded match'
        expectation.call( written, read )

        where:
        writerSchemaFile          | readerSchemaFile          | writerClosure | readerClosure | description                    || expectation
        'schemas/user-1.0.0.json' | 'schemas/user-1.0.0.json' | v100Writer    | v100Reader    | 'Reader matches writer'        || v100tov100Expectation
        'schemas/user-1.1.0.json' | 'schemas/user-1.0.0.json' | v110Writer    | v100Reader    | 'Writer adds additional field' || v110tov100Expectation
        'schemas/user-1.0.0.json' | 'schemas/user-1.1.0.json' | v100Writer    | v110Reader    | 'Reader adds additional field' || v100tov110Expectation
        'schemas/user-1.1.0.json' | 'schemas/user-1.2.0.json' | v110Writer    | v120Reader    | 'Reader splits field in two'   || v110tov120Expectation
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

    static def v110tov120RecordExpectation = { GenericRecord writer, GenericRecord reader ->
        (writer.get( 'name' ) as String == reader.get( 'name' ) as String) &&
                !writer.get( 'username' ) as String && // writer does not have a username
                reader.get( 'username' ) as String
    }

    @Unroll( 'Map-based: #description' )
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
        'schemas/user-1.1.0.json' | 'schemas/user-1.2.0.json' | v110MapBuilder | 'Reader splits field in two'   || v110tov120RecordExpectation
    }
}