package net.quasardb.kafka.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.Session;
import net.quasardb.kafka.common.resolver.Resolver;
import net.quasardb.kafka.common.resolver.TopicResolver;
import net.quasardb.kafka.common.resolver.StaticResolver;
import net.quasardb.kafka.common.resolver.ColumnResolver;

public class ConnectorUtils {

    private static final Logger log = LoggerFactory.getLogger(ConnectorUtils.class);

    public static final String CLUSTER_URI_CONFIG = "qdb.cluster";
    public static final String SECURITY_USERNAME_CONFIG = "qdb.security.username";
    public static final String SECURITY_USER_PRIVATE_KEY_CONFIG = "qdb.security.user_private_key";
    public static final String SECURITY_CLUSTER_PUBLIC_KEY_CONFIG = "qdb.security.cluster_public_key";
    public static final String TABLE_CONFIG = "qdb.table";
    public static final String TABLE_FROM_TOPIC_CONFIG = "qdb.table_from_topic";
    public static final String TABLE_FROM_COLUMN_CONFIG = "qdb.table_from_column";
    public static final String TABLE_AUTOCREATE_CONFIG = "qdb.table_autocreate";
    public static final String TABLE_AUTOCREATE_TAGS_CONFIG = "qdb.table_autocreate_tags";
    public static final String TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG = "qdb.table_autocreate_tags_column";
    public static final String TABLE_AUTOCREATE_SKELETON_CONFIG = "qdb.table_autocreate_skeleton";
    public static final String TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG = "qdb.table_autocreate_skeleton_column";


    /**
     * Parses table input config and emits a mapping of Kafka topics to
     * QuasarDB tables.
     */
    public static Map<String, String> parseTableFromTopic(Collection<String> xs) {
        Map<String, String> out = new HashMap();

        for (String x : xs) {
            String[] tokens = x.split("=");

            if (tokens.length != 2) {
                throw new DataException("Incorrectly formatted table config: expected 'topic=table', got: " + x);
            }

            out.put(tokens[0], tokens[1]);
        }

        return out;
    }

    public static Session connect(Map <String, Object> validatedProps) {
        String uri = (String)validatedProps.get(ConnectorUtils.CLUSTER_URI_CONFIG);

        if (validatedProps.containsKey(SECURITY_USERNAME_CONFIG) &&
            validatedProps.containsKey(SECURITY_USER_PRIVATE_KEY_CONFIG) &&
            validatedProps.get(SECURITY_USER_PRIVATE_KEY_CONFIG) != null &&
            validatedProps.containsKey(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG) &&
            validatedProps.get(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG) != null) {
            String userName = (String)validatedProps.get(SECURITY_USERNAME_CONFIG);
            String userPrivateKey = (String)validatedProps.get(SECURITY_USER_PRIVATE_KEY_CONFIG);
            String clusterPublicKey = (String)validatedProps.get(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG);
            log.info("Establishing secure connection");
            return Session.connect(new Session.SecurityOptions(userName,
                                                               userPrivateKey,
                                                               clusterPublicKey),
                                   uri);
        } else {
            log.warn("Establishing insecure connection");
            return Session.connect(uri);
        }
    }

    public static Resolver<String> createTableResolver(Map <String, Object> validatedProps) {
        if (validatedProps.containsKey(TABLE_FROM_COLUMN_CONFIG) &&
            validatedProps.get(TABLE_FROM_COLUMN_CONFIG) != null) {
            log.debug(TABLE_FROM_COLUMN_CONFIG + " set to true, using ColumnResolver");
            return new ColumnResolver<String>((String)validatedProps.get(TABLE_FROM_COLUMN_CONFIG));
        } else if (validatedProps.containsKey(TABLE_FROM_TOPIC_CONFIG) &&
                   (Boolean)validatedProps.get(TABLE_FROM_TOPIC_CONFIG) == Boolean.TRUE) {
            log.debug(TABLE_FROM_TOPIC_CONFIG + " set to true, using TopicResolver");
            return new TopicResolver();
        } else if (validatedProps.containsKey(TABLE_CONFIG) &&
                   validatedProps.get(TABLE_CONFIG) != null) {
            log.debug(TABLE_CONFIG + " provided, using StaticTableResolver");
            return new StaticResolver<String>((String)validatedProps.get(TABLE_CONFIG));
        } else  {
            log.debug("validatedProps: " + validatedProps.toString());
            throw new DataException("No valid TableResolving strategy could be determined, please correct your configuration");
        }
    }

    public static Resolver<String> createSkeletonTableResolver(Map <String, Object> validatedProps) {
        if (validatedProps.containsKey(TABLE_AUTOCREATE_SKELETON_CONFIG) &&
            validatedProps.get(TABLE_AUTOCREATE_SKELETON_CONFIG) != null) {
            log.debug(TABLE_AUTOCREATE_SKELETON_CONFIG + " provided, using StaticResolver");
            return new StaticResolver<String>((String)validatedProps.get(TABLE_AUTOCREATE_SKELETON_CONFIG));
        } else if (validatedProps.containsKey(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG) &&
                   validatedProps.get(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG) != null) {
            log.debug(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG + " provided, using ColumnResolver");
            return new ColumnResolver<String>((String)validatedProps.get(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG));
        } else {
            log.debug("No skeleton configuration");
            return null;
        }
    }

    public static Resolver<List<String>> createTableTagsResolver(Map <String, Object> validatedProps) {
        if (validatedProps.containsKey(TABLE_AUTOCREATE_TAGS_CONFIG) &&
            validatedProps.get(TABLE_AUTOCREATE_TAGS_CONFIG) != null) {
            log.debug(TABLE_AUTOCREATE_TAGS_CONFIG + " provided, using StaticResolver");
            return new StaticResolver<List<String>>((List<String>)validatedProps.get(TABLE_AUTOCREATE_TAGS_CONFIG));
        } else if (validatedProps.containsKey(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG) &&
                   validatedProps.get(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG) != null) {
            log.debug(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG + " provided, using ColumnResolver");
            return new ColumnResolver<List<String>>((String)validatedProps.get(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG));
        } else {
            log.debug("No table tags configuration");
            return null;
        }
    }
}
