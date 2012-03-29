package bixo.datum;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.PayloadDatum;

@SuppressWarnings("serial")
public class MailDatum extends PayloadDatum {

    public static String NAME_FN = fieldName(MailDatum.class, "name");
    public static String EMAIL_FN = fieldName(MailDatum.class, "email");

    public static final Fields FIELDS = new Fields(NAME_FN, EMAIL_FN).append(getSuperFields(MailDatum.class));

    /**
     * No argument constructor for use with FutureTask
     */
    public MailDatum() {
        super(FIELDS);
    }

    @Override
    public String toString() {
        return "{name: " + getName() + " email: " + getEmail() + " }";
    }

    public MailDatum(Fields fields) {
        super(fields);
        validateFields(fields, FIELDS);
    }

    public MailDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }

    public MailDatum(String name, String email) {
        super();

        setName(name);
        setEmail(email);
    }

    public String getEmail() {
        return _tupleEntry.getString(EMAIL_FN);
    }

    public String getName() {
        return _tupleEntry.getString(NAME_FN);
    }

    public void setName(String brandFn) {
        _tupleEntry.set(EMAIL_FN, brandFn);
    }

    public void setEmail(String categoryFn) {
        _tupleEntry.set(NAME_FN, categoryFn);
    }
}
