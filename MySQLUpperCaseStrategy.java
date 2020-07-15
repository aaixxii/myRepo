package jp.co.nec.aim.mm.constants;

import org.hibernate.cfg.ImprovedNamingStrategy;

public class MySQLUpperCaseStrategy extends ImprovedNamingStrategy {    
    
    private static final long serialVersionUID = -1734180543407308775L;

    @Override    
    public String tableName(String tableName) {
        return tableName.toUpperCase();         
    }
    
    @Override    
    public String columnName(String columnName) {
        return columnName.toUpperCase();        
    }
    
}
