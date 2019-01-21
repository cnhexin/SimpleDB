package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.Serializable;

public class Table implements Serializable {

        private static final long serialVersionUID = 1L;

        public DbFile file;
        
        public String name;
        public String pkeyField;

        public Table(DbFile file, String name, String pkeyField) {
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }

    }

