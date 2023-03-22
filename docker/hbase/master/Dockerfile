ARG HBASE_VERSION
FROM datasource/hbase-base-${HBASE_VERSION}

# Add entrypoint for hbase master.
ADD ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD [ "/entrypoint.sh" ]
