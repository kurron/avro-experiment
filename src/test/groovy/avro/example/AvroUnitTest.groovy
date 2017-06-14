package avro.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.avro.AvroFactory
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator
import groovy.transform.Canonical
import spock.lang.Specification

import java.util.concurrent.ThreadLocalRandom

/**
 * Exercise the various codec scenarios.
 */
class AvroUnitTest extends Specification {

    @Canonical
    static class Payload {
        boolean processed
        String text
        byte[] bytes
    }

    void 'exercise Jackson Avro support'() {

        given: 'an Avro schema and mapper'
        def mapper = new ObjectMapper( new AvroFactory() )
        def generator = new AvroSchemaGenerator()
        mapper.acceptJsonFormatVisitor( Payload.class, generator )
        def schema = generator.getGeneratedSchema()

        and: 'an Avro encoded instance'
        def text = Integer.toHexString( ThreadLocalRandom.current().nextInt( 0, Integer.MAX_VALUE ) )
        byte[] bytes = new byte[8]
        ThreadLocalRandom.current().nextBytes( bytes )
        def original = new Payload( text: text, bytes: bytes )
        byte[] encodedOriginal = mapper.writer( schema ).writeValueAsBytes( original )

        and: 'an Avro encoded decoy instance'
        def decoy = new Payload( text: 'decoy', bytes: 'decoy'.utf8Bytes )
        byte[] encodedDecoy = mapper.writer( schema ).writeValueAsBytes( decoy )

        when: 'the instance are recreated'
        Payload decodedDecoy = mapper.readerFor( Payload.class ).with( schema ).readValue( encodedDecoy )
        Payload decodedOriginal = mapper.readerFor( Payload.class ).with( schema ).readValue( encodedOriginal )

        then: 'the decoded decoy does not match the original'
        decodedDecoy != original

        and: 'the decoded original does match the original'
        decodedOriginal == original
    }
}
