using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Pgshardnet
{
    public class PhysicalShard
    {
        string _serverName, _serverDatabase, _userName, _password;
        int _serverPort;
        public PhysicalShard(string serverName, int serverPort, string serverDatabase, string userName, string password)
        {
            _serverName=serverName;
            _serverPort=serverPort;
            _serverDatabase=serverDatabase;
            _userName=userName;
            this.UserName=userName;
            _password=password;
        }

        public string UserName { get; private set; }

        public string Name
        {
            get
            {
                return String.Format("{3}@{0}\\{2}:{1}", _serverName, _serverPort, _serverDatabase, _userName);
            }
        }

        public OdbcConnection GetConnection()
        {
            return new OdbcConnection(String.Format("Driver={{PostgreSQL UNICODE(x64)}};Server={0};Port={1};Database={2};Uid={3};Pwd={4};sslmode=require;", _serverName, _serverPort, _serverDatabase, _userName, _password));
        }
    }
}
