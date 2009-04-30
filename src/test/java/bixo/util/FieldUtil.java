package bixo.util;

import cascading.tuple.Fields;

public class FieldUtil {

    public static Fields add(Fields fields, String... moreFieldNames) {
        Fields moreFields = new Fields(moreFieldNames);
        return fields.append(moreFields);
    }

    public static Fields combine(Fields fields, Fields moreFields) {
        return fields.append(moreFields);
    }

}
