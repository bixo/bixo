package bixo.datum;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import com.bixolabs.cascading.PayloadDatum;

@SuppressWarnings("serial")
public class ProductDatum extends PayloadDatum {

    public static String BRAND_FN = fieldName(ProductDatum.class, "brand");
    public static String CATEGORY_FN = fieldName(ProductDatum.class, "category");
    public static String DEPARTMENT_FN = fieldName(ProductDatum.class, "department");
    public static String MODEL_FN = fieldName(ProductDatum.class, "model");
    public static String SIZE_FN = fieldName(ProductDatum.class, "size");
    public static String COLOUR_FN = fieldName(ProductDatum.class, "colour");
    public static String PRICE_FN = fieldName(ProductDatum.class, "price");

    public static final Fields FIELDS = new Fields(BRAND_FN, CATEGORY_FN, DEPARTMENT_FN, MODEL_FN, SIZE_FN, COLOUR_FN, PRICE_FN).append(getSuperFields(ProductDatum.class));

    /**
     * No argument constructor for use with FutureTask
     */
    public ProductDatum() {
        super(FIELDS);
    }

    @Override
    public String toString() {
        return "{category: " + getCategory() + " brand: " + getBrand() 
                        + " department: " + getDepartment() + " model: " + getModel()
                        + " size: " + getSize() + " colour: " + getColour()
                        + " price: " + getPrice() + " }";
    }

    public ProductDatum(Fields fields) {
        super(fields);
        validateFields(fields, FIELDS);
    }

    public ProductDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }

    public ProductDatum(String brand, String category, String department, String model, String size, String colour, String price) {
        super();

        setBrand(brand);
        setCategory(category);
        setDepartment(department);
        setModel(model);
        setSize(size);
        setColour(colour);
        setPrice(price);
    }

    public String getBrand() {
        return _tupleEntry.getString(BRAND_FN);
    }

    public String getCategory() {
        return _tupleEntry.getString(CATEGORY_FN);
    }

    public String getDepartment() {
        return _tupleEntry.getString(DEPARTMENT_FN);
    }

    public String getModel() {
        return _tupleEntry.getString(MODEL_FN);
    }

    public String getSize() {
        return _tupleEntry.getString(SIZE_FN);
    }

    public String getColour() {
        return _tupleEntry.getString(COLOUR_FN);
    }

    public String getPrice() {
        return _tupleEntry.getString(PRICE_FN);
    }

    public void setBrand(String brandFn) {
        _tupleEntry.set(BRAND_FN, brandFn);
    }

    public void setCategory(String categoryFn) {
        _tupleEntry.set(CATEGORY_FN, categoryFn);
    }

    public void setDepartment(String departmentFn) {
        _tupleEntry.set(DEPARTMENT_FN, departmentFn);
    }

    public void setModel(String modelFn) {
        _tupleEntry.set(MODEL_FN, modelFn);
    }

    public void setSize(String sizeFn) {
        _tupleEntry.set(SIZE_FN, sizeFn);
    }

    public void setColour(String colourFn) {
        _tupleEntry.set(COLOUR_FN, colourFn);
    }

    public void setPrice(String priceFn) {
        _tupleEntry.set(PRICE_FN, priceFn);
    }

}
