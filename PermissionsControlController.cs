using Newtonsoft.Json;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Xml;
using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using DevExpress.CodeParser;
using DevExpress.DashboardExport.Map;
using System.Text.RegularExpressions;
using DevExpress.Data.Helpers;
using DevExpress.XtraRichEdit.Commands;
using ADWrapper;
using WebDAL;
using DevExpress.DashboardCommon;
using EmasMotesCore.Models.Models_EMAS;
using Microsoft.Net.Http.Headers;
using Octonica.ClickHouseClient;
using System.Data.SqlClient;

namespace EmasMotesCore.Controllers
{
    public class PermissionsControlController : ControllerBase
    {
        private readonly EMAS_Context _db;
        private readonly WebPluginUserActionsService _webPluginUserActionsService;
        private readonly EventCriticalityTypeService _eventCriticalityTypeService;
        private readonly WebPluginsService _webPluginsService;
        private readonly UsersService _usersService;
        private readonly IConfiguration _configuration;

        // Мапка кубов с дименшенами из XML (не объекты доступа)
        Dictionary<String, Dictionary<String, String>> dictDimensionByCubeXML = null;
        // Мапка виртуальных кубов с дименшенами из XML (не объекты доступа)
        Dictionary<String, Dictionary<String, String>> dictDimensionByVirtualCubeXML = null;
        //Мапка объектов доступа, которые в наличии в XMLROLE
        Dictionary<String, Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>> dictObjAccessByRoleXMLROL = null;
        //Мапка дименшенов с аннотациями (key - имя дименшена, value - sql-запрос)
        Dictionary<String, String> dictDimensionWithAnnotation = null;
        //Мапка Роль - Иерархия - Мембер
        Dictionary<String, Dictionary<String, Dictionary<String, bool>>> dictMemberByHierarhyByRoleXMLROL = null;
        //Мапка - хранилище изменений
        static Dictionary<string, Dictionary<string, string>> PermissionDataStorage = new Dictionary<string, Dictionary<string, string>>();
        //Мапка мемберов по ролям: путь мембера - разрешен/не разрешен
        Dictionary<String, Dictionary<String, bool>>  dictMemberPathAccessByRole = new Dictionary<String, Dictionary<String, bool>>();
        
        Dictionary<int, String> dictDimensionList = null;
        public PermissionsControlController(EMAS_Context db, WebPluginUserActionsService webPluginUserActionsService, EventCriticalityTypeService eventCriticalityTypeService, WebPluginsService webPluginsService, UsersService usersService, IConfiguration configuration)
        {
            _db = db;
            _webPluginUserActionsService = webPluginUserActionsService;
            _eventCriticalityTypeService = eventCriticalityTypeService;
            _webPluginsService = webPluginsService;
            _usersService = usersService;
            _configuration = configuration;
        }

        public object PermissionsControlInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _db.Roles.ToList();
                return DataSourceLoader.Load(model, loadOptions);
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public async Task<IActionResult> RolesInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _db.Roles.Select(f => new RoleDTO { Id = f.Id, Name = f.Name, Description = f.Description });
                return Json(await DataSourceLoader.LoadAsync(model, loadOptions));

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public async Task<IActionResult> ReportsInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                return Json(await DataSourceLoader.LoadAsync(
                    from r in _db.Reports
                    select new ReportDTO
                    {
                        Id = r.Id,
                        Name = r.Name
                    }
                    , loadOptions));

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public async Task<IActionResult> WebPluginsInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _db.Webplugins.Select(wp => new WebPluginDTO
                {
                    Id = wp.Id,
                    Parentid = wp.Parentid,
                    Caption = wp.Caption,
                    Name = wp.Name,
                    Viewname = wp.Viewname,
                    Image = wp.Image,
                    Sortorder = wp.Sortorder,
                    Ismenubar = wp.Ismenubar,
                    Isnavigationbar = wp.Isnavigationbar,
                    Issecret = wp.Issecret,
                    Trial = wp.Trial,
                    Isadmin = wp.Isadmin,
                    Updatedate = wp.Updatedate,
                });


                return Json(await DataSourceLoader.LoadAsync(model, loadOptions));
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public object SdsParentsWebplugins(DataSourceLoadOptions loadOptions)
        {
            List<WebPluginDTO> model = new List<WebPluginDTO>();
            try
            {

                foreach (var wp in _db.Webplugins
                    //  .Where(o => o.Parentid == 1)
                    //       .OrderBy(o => o.Name))
                    .OrderBy(o => o.Caption))
                {
                    int PID = -1;
                    if (!(wp.Parentid is null))
                        PID = (int)wp.Parentid;
                    model.Add(new WebPluginDTO
                    {
                        Id = wp.Id,
                        Parentid = PID,
                        Caption = wp.Caption,
                        Name = wp.Name,
                        Viewname = wp.Viewname
                    });
                }
            }
            catch (Exception ex)
            {
                //     logger.Error(ex);
                return BadRequest(null, ex);
            }
            return DataSourceLoader.Load(model, loadOptions);
        }
        public object GetWebPluginsList(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _webPluginsService.GetWebPluginsList().OrderBy(o => o.Caption);
                return DataSourceLoader.Load(model, loadOptions);
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public object GetWebPluginsUserActionsList(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _webPluginUserActionsService.GetWebPluginsUserActionsList().OrderBy(f => f.Name);
                return DataSourceLoader.Load(model, loadOptions);

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public object GetEventCriticalityTypesList(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _eventCriticalityTypeService.GetEventCriticalityTypeList().OrderBy(o => o.Name);
                return DataSourceLoader.Load(model, loadOptions);
            }
            catch (Exception ex)
            {
                //     logger.Error(ex);
                return BadRequest(null, ex);
            }
        }

        [HttpPost]
        public IActionResult RoleInsert(string values)
        {
            var newRole = new Role();
            JsonConvert.PopulateObject(values, newRole);
            try
            {

                _db.Roles.Add(newRole);
                _db.SaveChanges();

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        [HttpPut]
        public IActionResult RoleUpdate(int key, string values)
        {
            try
            {
                var editRoles = _db.Roles.First(p => p.Id == key);
                if (editRoles != null)
                {
                    JsonConvert.PopulateObject(values, editRoles);
                    _db.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        public object RoleDelete(int key)
        {
            try
            {
                var unit = _db.Roles.First(p => p.Id == key);
                if (unit != null && unit.Id > 0)
                {
                    _db.Roles.Remove(unit);
                    _db.SaveChanges();
                }
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        [HttpPost]
        public IActionResult WebPluginInsert(string values)
        {
            var newWebPlugin = new Webplugin();
            JsonConvert.PopulateObject(values, newWebPlugin);

            try
            {

                _db.Webplugins.Add(newWebPlugin);
                _db.SaveChanges();

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        [HttpPut]
        public IActionResult WebPluginUpdate(int key, string values)
        {
            try
            {
                var editWebplugins = _db.Webplugins.First(p => p.Id == key);
                if (editWebplugins != null)
                {
                    JsonConvert.PopulateObject(values, editWebplugins);

                    _db.SaveChanges();

                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        public object WebPluginDelete(int key)
        {
            try
            {
                var unit = _db.Webplugins.First(p => p.Id == key);
                if (unit != null && unit.Id > 0)
                {
                    _db.Webplugins.Remove(unit);
                    _db.SaveChanges();

                }
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        public async Task<IActionResult> RolesWebPluginsInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                var model = _webPluginsService.GetModel();
                return Json(await DataSourceLoader.LoadAsync(model, loadOptions));
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        [HttpPost]
        public IActionResult RolesWebPluginsInsert(string values)
        {
            try
            {
                EmasResult result = _webPluginsService.InsertElementWithResult(values);
                if ((result.Type != EmasResultType.Success || result.Type != EmasResultType.Empty) && result.ShowMessage)
                {
                    return new ConflictObjectResult(new { message = result.Message });
                }
            }
            catch(Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }

        [HttpPut]
        public IActionResult RolesWebPluginsUpdate(int key, string values)
        {
            try
            {
                EmasResult result = _webPluginsService.UpdateElementWithResult(key, values);
                if ((result.Type != EmasResultType.Success || result.Type != EmasResultType.Empty) && result.ShowMessage)
                {
                    return new ConflictObjectResult(new { message = result.Message });
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }

        [HttpDelete]
        public async Task<object> RolesWebPluginsDelete(int key)
        {
            var rolesWebPlugins = await _webPluginsService.GetModel().FirstOrDefaultAsync(p => p.Id == key);

            if (rolesWebPlugins != null && rolesWebPlugins.Id > 0)
            {
                _webPluginsService.DeleteElement(key);

            }
            return Ok();
        }

        public async Task<IActionResult> ReportsRolesInit(DataSourceLoadOptions loadOptions)
        {
            try
            {

                return Json(await DataSourceLoader.LoadAsync(
                    from rr in _db.Reportsroles
                    select
                    new ReportsRolesDTO
                    {
                        Id = rr.Id,
                        Reportid = rr.Reportid,
                        Roleid = rr.Roleid
                    },
                    loadOptions));
            }
            catch (Exception ex)
            {
                //    logger.Error(ex);
                return BadRequest(null, ex);
            }
        }
        [HttpPost]
        public IActionResult ReportsRolesInsert(string values)
        {

            var newReportsroles = new Reportsrole();
            JsonConvert.PopulateObject(values, newReportsroles);
            try
            {

                _db.Reportsroles.Add(newReportsroles);
                _db.SaveChanges();

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        [HttpPut]
        public IActionResult ReportsRolesUpdate(int key, string values)
        {
            try
            {
                var editReportsroles = _db.Reportsroles.First(p => p.Id == key);
                if (editReportsroles != null)
                {
                    JsonConvert.PopulateObject(values, editReportsroles);
                    _db.SaveChanges();

                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        public object ReportsRolesDelete(int key)
        {
            try
            {
                var unit = _db.Reportsroles.First(p => p.Id == key);
                if (unit != null && unit.Id > 0)
                {
                    _db.Reportsroles.Remove(unit);
                    _db.SaveChanges();

                }
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        public async Task<IActionResult> DashboardsRolesInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                return Json(await DataSourceLoader.LoadAsync(
                    from dr in _db.DashRolesdashboards
                    select new DashRoleDashboardDTO
                    {
                        Id = dr.Id,
                        DashboardId = dr.Dashboardid,
                        RoleId = dr.Roleid,
                    }
                    , loadOptions));

            }
            catch (Exception ex)
            {
                //    logger.Error(ex);
                return BadRequest(null, ex);
            }
        }
        [HttpPost]
        public IActionResult DashboardsRolesInsert(string values)
        {
            var newDashboardRole = new DashRolesdashboard();
            JsonConvert.PopulateObject(values, newDashboardRole);
            try
            {

                _db.DashRolesdashboards.Add(newDashboardRole);
                _db.SaveChanges();

            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        [HttpPut]
        public IActionResult DashboardsRolesUpdate(int key, string values)
        {
            try
            {
                var editDashboardRole = _db.DashRolesdashboards.First(p => p.Id == key);
                if (editDashboardRole != null)
                {
                    JsonConvert.PopulateObject(values, editDashboardRole);

                    _db.SaveChanges();

                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return Ok();
        }
        public object DashboardsRolesDelete(int key)
        {
            try
            {
                var unit = _db.DashRolesdashboards.First(p => p.Id == key);
                if (unit != null && unit.Id > 0)
                {
                    _db.DashRolesdashboards.Remove(unit);
                    _db.SaveChanges();

                }
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        public async Task<IActionResult> DashboardsInit(DataSourceLoadOptions loadOptions)
        {
            try
            {
                return Json(await DataSourceLoader.LoadAsync(
                    from dash in _db.DashDashboards
                    select new DashboardNameDTO
                    {
                        Id = dash.Id,
                        Caption = dash.Caption,
                    }
                    , loadOptions));
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }

        public object LookupWebPlugins(DataSourceLoadOptions loadOptions)
        {
            List<LookupItem> model = new List<LookupItem>();
            try
            {
                string strQuery = "SELECT ID, coalesce(ParentID, '-1') AS ParentID, Name, Caption, " +
                                  " ComponentPath, Image, coalesce(IsMenuBar, false) IsMenuBar, " +
                                  " coalesce(IsNavigationBar, false) IsNavigationBar, coalesce(IsToolBar,false) IsToolBar," +
                                  " coalesce(IsSecret, false) IsSecret, SortOrder, Trial,UpdateDate, IsAdmin, " +
                                  " Caption_Eng FROM WebPlugins ORDER BY Caption";
                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandType = CommandType.Text;
                    sql.CommandText = strQuery;
                    NpgsqlDataReader reader = sql.ExecuteReader();
                    while (reader.Read())
                    {
                        model.Add(new LookupItem { Id = long.Parse(reader["ID"].ToString()), Name = reader["Caption"].ToString() });
                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return DataSourceLoader.Load(model, loadOptions);
        }

        public object WebPluginIndicatorInit(DataSourceLoadOptions loadOptions)
        {
            List<WebPluginIndicator> model = new List<WebPluginIndicator>();
            try
            {
                string strQuery = "SELECT ID, Name, Caption, WebPluginId, IsActive FROM WebPluginIndicators ORDER BY WebPluginId, ID ";

                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandType = CommandType.Text;
                    sql.CommandText = strQuery;
                    NpgsqlDataReader reader = sql.ExecuteReader();
                    while (reader.Read())
                    {
                        WebPluginIndicator item = new WebPluginIndicator();
                        item.Id = long.Parse(reader["ID"].ToString());
                        item.Webpluginid = long.Parse(reader["Webpluginid"].ToString());
                        item.Name = reader["Name"].ToString();
                        item.Caption = reader["Caption"].ToString();
                        item.Isactive = reader["IsActive"] == DBNull.Value ? false : Convert.ToBoolean(reader["IsActive"]);
                        model.Add(item);
                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return DataSourceLoader.Load(model, loadOptions);
        }

        public object WebPluginIndicatorInsert(string values)
        {
            WebPluginIndicator item = new WebPluginIndicator();
            JsonConvert.PopulateObject(values, item);

            try
            {

                _db.WebPluginIndicators.Add(item);
                _db.SaveChanges();

            }
            catch (Exception e)
            {
                return BadRequest(null, e);
            }
            return Ok();
        }
        public IActionResult WebPluginIndicatorUpdate(int key, string values)
        {
            try
            {
                var item = _db.WebPluginIndicators.First(p => p.Id == key);
                if (item != null)
                {
                    JsonConvert.PopulateObject(values, item);

                    _db.SaveChanges();

                }
            }
            catch (Exception e)
            {
                return BadRequest(null, e);
            }
            return Ok();
        }

        public IActionResult WebPluginIndicatorDelete(int key)
        {
            try
            {
                var item = _db.WebPluginIndicators.First(p => p.Id == key);
                if (item != null && item.Id > 0)
                {
                    _db.WebPluginIndicators.Remove(item);
                    _db.SaveChanges();

                }
            }
            catch (Exception e)
            {
                return BadRequest(null, e);
            }
            return Ok();
        }

        public object GetUsers(DataSourceLoadOptions loadOptions)
        {
            List<LookupItem> model = new List<LookupItem>();
            try
            {
                string strQuery = "SELECT coalesce(u.LastName,'') || ' ' || coalesce(u.FirstName,'') || ' ' || coalesce(u.MiddleName,'') || ', ' || u.Login as Login " +
                                  " FROM Users u where IsActive = true AND u.Login is NOT NULL and u.Login <> '' ORDER BY u.LastName";

                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandType = CommandType.Text;
                    sql.CommandText = strQuery;
                    NpgsqlDataReader reader = sql.ExecuteReader();
                    int id = 1;
                    while (reader.Read())
                    {
                        model.Add(new LookupItem { Id = id, Name = reader["Login"].ToString() });
                        id++;
                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
            return DataSourceLoader.Load(model, loadOptions);
        }

        [HttpPost]
        public async Task<IActionResult> UserChanged(string userLogin)
        {
            //await HttpContext.SignOutAsync(
            //   CookieAuthenticationDefaults.AuthenticationScheme);
            if (ModelState.IsValid)
            {            
                ApplicationUser user = null;
                var _dbUser = _usersService.GetUser(u => u.Login.ToLower() == userLogin.Substring(userLogin.LastIndexOf(",") + 2).ToLower() &&
                                     u.Isactive == true
                                     );
                if (_dbUser != null)
                {
                    var oldUser = HttpContext.Session.GetObjectFromJson<long>("UserID");
                    user = new ApplicationUser() { Name = _dbUser.Login, FullName = _dbUser.FullName, RoleId = _dbUser.Roleid, UserId = _dbUser.Id };
                    HttpContext.Session.SetObjectAsJson("UserID", user.UserId);
                    HttpContext.Session.SetObjectAsJson("UserId", user.UserId);
                    HttpContext.Session.SetObjectAsJson("RoleId", user.RoleId);
                    HttpContext.Session.SetObjectAsJson("UserLogin", user.Name);
                    HttpContext.Session.SetObjectAsJson("UserFullName", user.FullName);
                    HttpContext.Session.SetObjectAsJson("IsSwitched", true);
                    AddJournalDb_EMAS(UserAction.System_login, "Вход в систему пользователя: ID = " + oldUser + " под ID = " + HttpContext.Session.GetObjectFromJson<long>("UserID"), null, "EventsLog");
                }

                //var claims = new List<Claim>
                //{
                //    new Claim(ClaimTypes.Name, user.Name),
                //    new Claim(ClaimTypes.Role, "Administrator"),
                //};

                //var claimsIdentity = new ClaimsIdentity(
                //    claims, CookieAuthenticationDefaults.AuthenticationScheme);

                //var authProperties = new AuthenticationProperties
                //{ };

                //await HttpContext.SignInAsync(
                //    CookieAuthenticationDefaults.AuthenticationScheme,
                //    new ClaimsPrincipal(claimsIdentity),
                //    authProperties);

                return Ok();
            }

            // Something failed. Redisplay the form.
            // using static EmasMotesCore.Account.LoginModel;
            EmasMotesCore.Account.LoginModel model = new EmasMotesCore.Account.LoginModel() { Name = "", Password = "" };
            return View("Login", model);
        }


        //Список ролей из базы данных
        private List<Role> getRoleList()
        {

            List<Role> model = new List<Role>();
            try
            {
                model = _db.Roles.OrderBy(p => p.Name).ToList();
                //model = _db.Roles.Where(p => p.Name == "Guest").OrderBy(p => p.Name).ToList();
                //model = _db.Roles.Where(p => p.Name == "Администраторы EMAS").OrderBy(p => p.Name).ToList();
                //model = _db.Roles.Where(p => p.Name == "Администраторы EMAS" || p.Name == "Guest").OrderBy(p => p.Name).ToList();
            }
            catch (Exception ex)
            {
                throw new Exception("Ошибка загрузки таблицы roles");
            }
            return model;

        }

        /*
        // УСТАРЕВШИЕ ФУНКЦИИ
        //Список станций
        private List<Station> getStationList()
        {
            List<Station> model = new List<Station>();
            try
            {
                model = _db.Stations.OrderBy(p => p.Name).ToList();
            }
            catch (Exception ex)
            {
                throw new Exception("Ошибка загрузки таблицы stations");
            }
            return model;
        }

        private List<Stationrole> getStationRoleList()
        {

            List<Stationrole> listStationrole = null;
            try
            {

                var query = from sr in _db.Stationroles
                            join r in _db.Roles on sr.Roleid equals r.Id
                            join st in _db.Stations on sr.Stationid equals st.Id
                            select new Stationrole
                            {
                                Id = sr.Id,
                                Roleid = r.Id,
                                Stationid = st.Id,
                                Role = new Role { Id = r.Id, Name = r.Name },
                                Station = new Station { Id = st.Id, Name = st.Name }

                            };

                listStationrole = query.ToList();

            }
            catch (Exception ex)
            {
                throw new Exception("Ошибка загрузки таблицы stationroles");
            }
            return listStationrole;
        }


        //Мапка "Роли по станциям"
        private Dictionary<string, Dictionary<string, string>> getStationRoleDict()
        {

            Dictionary<string, Dictionary<string, string>> model = new Dictionary<string, Dictionary<string, string>>();

            List<Stationrole> listStationrole = getStationRoleList();

            foreach (Stationrole itemStationrole in listStationrole)
            {

                if (!model.ContainsKey(itemStationrole.Station.Name))
                {
                    Dictionary<string, string> newDictRole = new Dictionary<string, string>();
                    newDictRole.Add(itemStationrole.Role.Name, itemStationrole.Role.Name);
                    model.Add(itemStationrole.Station.Name, newDictRole);
                }
                else
                {
                    Dictionary<string, string> dictRole = model[itemStationrole.Station.Name];
                    dictRole.Add(itemStationrole.Role.Name, itemStationrole.Role.Name);
                }

            }

            return model;
        }
        */

        public object OlapAccessGridColumnInit(string mode, DataSourceLoadOptions loadOptions)  //isCube
        {
            List<Role> listRole = getRoleList();
            List<DataGridColumns> result = new List<DataGridColumns>();
            String txtCaption = "";

            switch (mode)
            {
                case "cube":
                    HttpContext.Session.Remove("CubeOlapAccessGUID");
                    HttpContext.Session.SetObjectAsJson("CubeOlapAccessGUID", Guid.NewGuid().ToString());
                    txtCaption = "Куб/Роль";
                    break;
                case "dimension":
                    HttpContext.Session.Remove("DimensionOlapAccessGUID");
                    HttpContext.Session.SetObjectAsJson("DimensionOlapAccessGUID", Guid.NewGuid().ToString());
                    txtCaption = "Измерение/Роль";
                    break;
                //case "rolestation":
                //    HttpContext.Session.Remove("RoleStationOlapAccessGUID");
                //    HttpContext.Session.SetObjectAsJson("RoleStationOlapAccessGUID", Guid.NewGuid().ToString());
                //    txtCaption = "Станция/Роль";
                //    break;
                case "attributeolap":
                    HttpContext.Session.Remove("AttributeOlapAccessGUID");
                    HttpContext.Session.SetObjectAsJson("AttributeOlapAccessGUID", Guid.NewGuid().ToString());
                    txtCaption = "Аттрибуты";
                    break;
            }

            result.Add(new DataGridColumns() { DataField = "OlapName", Caption = txtCaption, DataType = GridColumnDataType.String, Fixed = true, ReadOnly = false, Visible = true });

            //Add columns with roles
            try
            {
                foreach (Role item in listRole)
                {
                    result.Add(new DataGridColumns() { DataField = roleIdGenerate(item.Name), Caption = item.Name, DataType = GridColumnDataType.Boolean, Fixed = false, ReadOnly = false, Visible = true });
                }
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.ExceptionErrorMessage() + "  && OlapAccessGridColumnInit, 1" });
            }

            switch (mode)
            {
                case "cube": return PartialView("PermissionsControlCubeGrid", result);
                case "dimension": return PartialView("PermissionsControlDimensionGrid", result);
                //case "rolestation": return PartialView("PermissionsControlRolesStationsGrid", result);
            }
            return new DataTable();
        }

        private string roleIdGenerate(string roleName)
        {
            return roleName.Replace(".", "_a26da675_f228_4360_fff_").Replace(" ", "_87cd_4a9e753cf5db_");
        }

        private string getShemaFilePath()
        {
            string? filePath = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPschemaPath")?.Value;
            return filePath;
        }
        private string getShemaFilePathWithRoot()
        {
            string? filePath = HttpContext.Session.GetObjectFromJson<string>("ShemaFileSelected");
            if(filePath == null ) {
                //filePath = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPschemaPath")?.Value;
                return null;
            }

            string? filePathRoot = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAP_RootPatch")?.Value;
            if(filePathRoot == null)
            {
               throw new Exception("Не удалось получить из базы данных корневой путь к OLAP-файлам && getShemaFilePath, 1"); 
            }


            return filePathRoot+filePath;
        }

        public object GetShemaFilePathListBD(DataSourceLoadOptions loadOptions)
        {
            Dictionary<long, string> model = _db.VSystemsettings.Select(input => input) //s => new{ s.Id, s.Value, s.Name } )
                .Distinct().Where(s => s.Name == "OLAPschemaPathM")
                .OrderBy(input => input.Id)
                .ToDictionary(x => (long)x.Id, x => x.Value)
                .OrderBy(i=>i.Key)
                .Select((entry, i) => new { entry.Value, i })
                .ToDictionary(pair=>(long)pair.i, pair=>pair.Value);

            return DataSourceLoader.Load(model, loadOptions);
        }
//123456  ################################################################################

        public object GetShemaFilePathList(DataSourceLoadOptions loadOptions)
        {
            try{
            return DataSourceLoader.Load(GetShemaFilePathDict(), loadOptions);
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }
        }
        private string getUserName_OlapServerRestart()
        {

            string value = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPserverRestartUserName").Value;
            return value;

        }
        private string getPassword_OlapServerRestart()
        {
            string value = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPserverRestartPassword").Value;
            return value;
        }
        private string getUrl_OlapServerRestart()
        {
            string value = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAPserverRestartUrl").Value;
            return value;
        }
        private Schema getShema()
        {
            //string filePath = getShemaFilePath();  // без селектора схем (устарело)
            string filePath = getShemaFilePathWithRoot();  //С селектором схем
            if(filePath == null) return null;

            String strSchemaFile = "";
            Schema objSchema = null;

            try
            {
                strSchemaFile = XDocument.Load(filePath).ToString();
            }
            catch (Exception ex)
            {
                throw new Exception("Не найден файл " + filePath + "  && getShema, 1");
            }

            try
            {
                XmlRootAttribute xmlRoot = new XmlRootAttribute
                {
                    ElementName = "Schema",
                    IsNullable = true
                };

                XmlSerializer serializer = new XmlSerializer(typeof(Schema), xmlRoot);


                using (StringReader reader = new StringReader(strSchemaFile))
                {
                    objSchema = (Schema)serializer.Deserialize(reader);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && getShema, 2");
            }

            foreach (var role in objSchema.Role)
            {
                foreach (var sg in role.SchemaGrant)
                {
                    sg.CubeGrant = sg.CubeGrant.ToList().GroupBy(cg => cg.cube).Select(g => g.First()).ToArray();
                }
            }

            return objSchema;
        }

        //Получение данных для таблицы (без передачи выбранной OLAP-схемы, устарела)
        public object OlapAccessGridInitData(string mode, DataSourceLoadOptions loadOptions)
        {
            DataTable result = new DataTable();
            try
            {
                switch (mode)
                {
                    case "cube":
                        result = GetCubeGridData();
                        break;
                    case "dimension":
                        result = GetDimensionGridData();
                        break;
                        /*
                    case "rolestation":
                        result = GetRolestationGridData();
                        break;
                        */
                }
            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }

            IEnumerable<Dictionary<string, object>> model = ToDataTableList(result);
            return DataSourceLoader.Load(model, loadOptions);
        }

        //Получение данных для таблицы (с передачей выбранной OLAP-схемы)
        public object OlapAccessGridInitDataM(string mode, string shemafile, DataSourceLoadOptions loadOptions)
        {
            HttpContext.Session.SetObjectAsJson("ShemaFileSelected", shemafile);
            return OlapAccessGridInitData(mode, loadOptions);
        }

        //Получить мапку ролей из схемы
        public Dictionary<String, SchemaRole> getMapSchemaRole(Schema objSchema)
        {
            Dictionary<String, SchemaRole> mapRole = new Dictionary<String, SchemaRole>();

            if (objSchema == null) return mapRole;
            if (objSchema.Role == null) return mapRole;

            foreach (SchemaRole item in objSchema.Role.ToList())
            {
                mapRole.Add(item.name, item);
            }
            return mapRole;
        }

        //Получить мапку ролей из БД
        public Dictionary<String, Role> getMapRoleBD(List<Role> listRole)
        {
            Dictionary<String, Role> mapRole = new Dictionary<String, Role>();

            if (listRole == null) return mapRole;

            foreach (Role item in listRole)
            {
                mapRole.Add(item.Name, item);
            }
            return mapRole;
        }

        //Заполнение мапки dictObjAccessByRoleXMLROL объектами доступа из схемы XMLROLE
        //(до кубов включительно)
        private int fillDictObjAccessByRoleXMLROL(Schema objSchema)
        {
            int countCheck = 0;

            dictObjAccessByRoleXMLROL = new Dictionary<String, Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>>();

            if (objSchema.Role == null) return countCheck;
            List<SchemaRole> lstShemRole = objSchema.Role.ToList();

            //Список дименшенов (иерархий), у которых есть аннотации
            Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);

            foreach (SchemaRole itemShemRole in lstShemRole)
            {

                if (itemShemRole.SchemaGrant[0] == null) continue;
                if (itemShemRole.SchemaGrant[0].CubeGrant == null) continue;

                Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> dictCubeXMLROL = new Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>();
                dictObjAccessByRoleXMLROL.Add(itemShemRole.name, dictCubeXMLROL);

                foreach (SchemaRoleSchemaGrantCubeGrant itemCube in itemShemRole.SchemaGrant[0].CubeGrant.ToList())
                {

                    if (itemCube.HierarchyGrant == null) continue;
                    Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant> dictHierarchyXMLROL = new Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                    if (!dictCubeXMLROL.ContainsKey(itemCube.cube))
                    {
                        dictCubeXMLROL.Add(itemCube.cube, dictHierarchyXMLROL);
                    }
                }
            }

            return countCheck;
        }


        private SchemaRoleSchemaGrantCubeGrantHierarchyGrant createHierarhyForDictObjAccessByRoleXMLROL(String hierarhy
                                                                    , SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess hierarhyAccess
                                                                    , Dictionary<String, bool> memberBase
                                                                    , SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant[] memberGrantRole
                                                                    )
        {

            SchemaRoleSchemaGrantCubeGrantHierarchyGrant objHierarchyXMLROL = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant
            {
                hierarchy = hierarhy,
                rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial,
                access = hierarhyAccess
            };

            if (hierarhyAccess == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
            {

                if (memberBase != null)
                {

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listObjMember = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                    foreach (KeyValuePair<String, bool> entry in memberBase)
                    {

                        SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                        {
                            member = entry.Key,
                            access = (entry.Value == true) ? SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all
                                                            : SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none
                        };
                        listObjMember.Add(objMember);
                    }
                    objHierarchyXMLROL.MemberGrant = listObjMember.ToArray();
                }
                else if (memberGrantRole != null)
                {

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listObjMember = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                    foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMember in memberGrantRole.ToList())
                    {
                        SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                        {
                            member = itemMember.member,
                            access = itemMember.access
                        };
                        listObjMember.Add(objMember);
                    }
                    objHierarchyXMLROL.MemberGrant = listObjMember.ToArray();
                }
            }

            return objHierarchyXMLROL;
        }

        Dictionary<String, bool> dictCubeVisible = new  Dictionary<String, bool>();

        //Заполнение мапки с кубами и дименшенами из XML (не объекты доступа)
        private void filldictDimensionByCubeXML(Schema objSchema)
        {

            if (dictDimensionByCubeXML != null) return;
            dictDimensionByCubeXML = new Dictionary<String, Dictionary<String, String>>();
            if( objSchema.Cube == null ) return;

            foreach (SchemaCube itemCub in objSchema.Cube.ToList())
            {
                if(dictCubeVisible.ContainsKey(itemCub.name))
                {
                    if (itemCub.visible == false)
                        dictCubeVisible[itemCub.name] = false;
                    else
                        dictCubeVisible[itemCub.name] = true;
                }
                else
                {
                    if (itemCub.visible == false)
                        dictCubeVisible.Add(itemCub.name, false);
                    else
                        dictCubeVisible.Add(itemCub.name, true);
                }
                if (itemCub.visible == false) continue; //Оставляем только видимые кубы

                Dictionary<String, String> dictDim = new Dictionary<String, String>();

                for (int ind = 0; ind < itemCub.Items.Length; ind++)
                {
                    dictDim.Add(((DimensionUsage)itemCub.Items[ind]).name, ((DimensionUsage)itemCub.Items[ind]).name);
                }
                dictDimensionByCubeXML.Add(itemCub.name, dictDim);
            }
            return;
        }

        //Заполнение мапки с виртуальными кубами и дименшенами из XML (не объекты доступа)
        private void filldictDimensionByVirtualCubeXML(Schema objSchema)
        {

            if (dictDimensionByVirtualCubeXML != null) return;
            
            dictDimensionByVirtualCubeXML = new Dictionary<String, Dictionary<String, String>>();

            if (objSchema.VirtualCube == null) return;
            foreach (SchemaVirtualCube itemVCub in objSchema.VirtualCube.ToList())
            {
                //if (itemVCub.visible == false) continue; //Оставляем только видимые кубы
                dictCubeVisible.Add(itemVCub.name, true);

                Dictionary<String, String> dictDim = new Dictionary<String, String>();
                List<SchemaVirtualCubeVirtualCubeDimension> listVDim = itemVCub.VirtualCubeDimension.ToList();

                foreach (SchemaVirtualCubeVirtualCubeDimension itemVDim in listVDim)
                {
                    dictDim.Add(itemVDim.name, itemVDim.name);
                }
                dictDimensionByVirtualCubeXML.Add(itemVCub.name, dictDim);
            }

            return;
        }

        private SchemaRole createNewSchemaRole(string roleName)
        {

            SchemaRoleSchemaGrant objSchemaRoleSchemaGrant = new SchemaRoleSchemaGrant { access = SchemaRoleSchemaGrantAccess.all };
            SchemaRole objSchemaRole = new SchemaRole { name = roleName };
            List<SchemaRoleSchemaGrant> listSchemaRoleSchemaGrant = new List<SchemaRoleSchemaGrant>() { objSchemaRoleSchemaGrant };
            objSchemaRole.SchemaGrant = listSchemaRoleSchemaGrant.ToArray();
            return objSchemaRole;
        }

        // Проверка целостности кубов в роли
        private int checkCubeXMLROL(Schema objSchema, ref SchemaRole schemaRoleXMLROL)
        {

            int countCheck = 0;
            Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> dictObjAccessCube = dictObjAccessByRoleXMLROL[schemaRoleXMLROL.name];

            if (schemaRoleXMLROL.SchemaGrant == null)
            {
                SchemaRoleSchemaGrant objSchemaRoleSchemaGrant = new SchemaRoleSchemaGrant();
                objSchemaRoleSchemaGrant.access = SchemaRoleSchemaGrantAccess.all;
                List<SchemaRoleSchemaGrant> listSchemaRoleSchemaGrant = new List<SchemaRoleSchemaGrant>() { objSchemaRoleSchemaGrant };
                schemaRoleXMLROL.SchemaGrant = listSchemaRoleSchemaGrant.ToArray();
            }

            List<SchemaRoleSchemaGrantCubeGrant> listCubeXMLROL = null;
            if (schemaRoleXMLROL.SchemaGrant[0].CubeGrant == null)
            {
                listCubeXMLROL = new List<SchemaRoleSchemaGrantCubeGrant>();
                schemaRoleXMLROL.SchemaGrant[0].CubeGrant = listCubeXMLROL.ToArray();
            }
            else
            {
                listCubeXMLROL = schemaRoleXMLROL.SchemaGrant[0].CubeGrant.ToList();
            }

            //Проверка кубов - удаляем лишнее
            if (listCubeXMLROL.Count > 0)
            {
                List<SchemaRoleSchemaGrantCubeGrant> listCubeXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrant>();

                //Формируем список на удаление
                foreach (SchemaRoleSchemaGrantCubeGrant itemCubeXMLROL in listCubeXMLROL)
                {
                    if (!dictDimensionByCubeXML.ContainsKey(itemCubeXMLROL.cube) && !dictDimensionByVirtualCubeXML.ContainsKey(itemCubeXMLROL.cube))
                    {
                        listCubeXMLROLforDelete.Add(itemCubeXMLROL);
                    }
                }
                //Удаляем
                foreach (SchemaRoleSchemaGrantCubeGrant itemCubeXMLROL in listCubeXMLROLforDelete)
                {
                    listCubeXMLROL.Remove(itemCubeXMLROL);

                    if (dictObjAccessCube.ContainsKey(itemCubeXMLROL.cube))
                    {
                        dictObjAccessCube.Remove(itemCubeXMLROL.cube);
                        countCheck++;
                    }
                }
                schemaRoleXMLROL.SchemaGrant[0].CubeGrant = listCubeXMLROL.ToArray();
            }

            //Добавляем
            //Кубы
            listCubeXMLROL = schemaRoleXMLROL.SchemaGrant[0].CubeGrant.ToList();
            foreach (KeyValuePair<String, Dictionary<String, String>> entry in dictDimensionByCubeXML)
            {

                SchemaRoleSchemaGrantCubeGrant objCubeXMLROL = null;
                if (!dictObjAccessCube.ContainsKey(entry.Key))
                {
                    objCubeXMLROL = new SchemaRoleSchemaGrantCubeGrant();
                    objCubeXMLROL.cube = entry.Key;
                    objCubeXMLROL.access = SchemaRoleSchemaGrantCubeGrantAccess.none;
                    listCubeXMLROL.Add(objCubeXMLROL);
                    dictObjAccessCube.Add(entry.Key, new Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>());
                    countCheck++;
                }

            }
            //Виртуальные кубы
            foreach (KeyValuePair<String, Dictionary<String, String>> entry in dictDimensionByVirtualCubeXML)
            {

                SchemaRoleSchemaGrantCubeGrant objCubeXMLROL = null;
                if (!dictObjAccessCube.ContainsKey(entry.Key))
                {
                    objCubeXMLROL = new SchemaRoleSchemaGrantCubeGrant();
                    objCubeXMLROL.cube = entry.Key;
                    objCubeXMLROL.access = SchemaRoleSchemaGrantCubeGrantAccess.none;
                    listCubeXMLROL.Add(objCubeXMLROL);
                    dictObjAccessCube.Add(entry.Key, new Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>());
                    countCheck++;
                }

            }

            schemaRoleXMLROL.SchemaGrant[0].CubeGrant = listCubeXMLROL.ToArray();
            int aa = 0;
            //Проверяем кубы
            for (int ind = 0; ind < schemaRoleXMLROL.SchemaGrant[0].CubeGrant.Length; ind++)
            {

                countCheck += checkHierarchyXMLROL(objSchema, schemaRoleXMLROL.name, ref schemaRoleXMLROL.SchemaGrant[0].CubeGrant[ind]);
                countCheck += checkDimensionXMLROL(objSchema, schemaRoleXMLROL.name, ref schemaRoleXMLROL.SchemaGrant[0].CubeGrant[ind]);
                aa++;
            }

            schemaRoleXMLROL.SchemaGrant[0].CubeGrant = listCubeXMLROL.ToArray();

            return countCheck;
        }
        // Проверка целостности дименшенов в роли и в кубе
        private int checkDimensionXMLROL(Schema objSchema, string roleName, ref SchemaRoleSchemaGrantCubeGrant cubeXMLROL)
        {
            int countCheck = 0;
            
            List<SchemaRoleSchemaGrantCubeGrantDimensionGrant> listDimensionXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrantDimensionGrant>();
            List<SchemaRoleSchemaGrantCubeGrantDimensionGrant> listDimensionXMLROL = null;
            Dictionary<String, String> dictHierarchyXMLROL = new Dictionary<String, String>();

            try
            {
                //Заполняем словарь иерархий с правами доступа
                
                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemDimension in cubeXMLROL.HierarchyGrant.ToList())
                {
                    if(dictHierarchyXMLROL.ContainsKey(itemDimension.hierarchy)) continue;

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLres = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                    foreach (var resultHierarchy in cubeXMLROL.HierarchyGrant.ToList().Where(s => s.hierarchy == itemDimension.hierarchy))
                    {
                        listHierarchyXMLres.Add(resultHierarchy);
                    }

                    if(listHierarchyXMLres.Count == 1)
                    {
                        dictHierarchyXMLROL.Add(itemDimension.hierarchy, itemDimension.access.ToString());
                    } else if(listHierarchyXMLres.Count > 1) { 
                        int cntMembers = 0;
                        SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess hAccess = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;

                        foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant reslst in listHierarchyXMLres)
                        { 
                            if(reslst.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom){ 
                                if(reslst.MemberGrant!=null)
                                {
                                cntMembers = reslst.MemberGrant.Count();
                                } else cntMembers = 0;
                            } else { 
                                hAccess = reslst.access;
                            }
                        }
                        //если мемберов больше 0, то доступ = all, иначе доступ = hAccess
                        if(cntMembers>0)
                        { 
                            dictHierarchyXMLROL.Add(itemDimension.hierarchy, SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all.ToString());

                        }
                        else
                        {
                            dictHierarchyXMLROL.Add(itemDimension.hierarchy, hAccess.ToString());
                        }
                    
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkDimensionXMLROL, 1");
            }
       

            //Удаляем лишнее
            try
            {

                if (cubeXMLROL.DimensionGrant != null)
                {
                    listDimensionXMLROL = cubeXMLROL.DimensionGrant.ToList();
                    foreach (SchemaRoleSchemaGrantCubeGrantDimensionGrant itemDimension in listDimensionXMLROL)
                    {
                        if (!dictHierarchyXMLROL.ContainsKey(itemDimension.dimension))
                        { // по любому в dictHierarchyXMLcb иерархия обязана быть!
                            listDimensionXMLROLforDelete.Add(itemDimension);
                        }

                    }

                    foreach (SchemaRoleSchemaGrantCubeGrantDimensionGrant itemDimensionDel in listDimensionXMLROLforDelete)
                    {
                        listDimensionXMLROL.Remove(itemDimensionDel);
                        countCheck++;
                    }

                } 
                else 
                { 
                    listDimensionXMLROL = new List<SchemaRoleSchemaGrantCubeGrantDimensionGrant>();
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkDimensionXMLROL, 2");
            }

            //Добавляем недостающее
            try
            {

                foreach (KeyValuePair<String, String> entry in dictHierarchyXMLROL)
                {

                    //SchemaRoleSchemaGrantCubeGrantDimensionGrant objDimensionXMLROL = null;

                    bool bResOk = false;
                    foreach (SchemaRoleSchemaGrantCubeGrantDimensionGrant itemDimension in listDimensionXMLROL.Where(s => s.dimension == entry.Key))
                    {
                        bResOk = true;

                        if(!itemDimension.access.ToString().Equals(entry.Value) ){ 
                            
                            if(entry.Value.Equals("all"))
                            { 
                                itemDimension.access = SchemaRoleSchemaGrantCubeGrantDimensionGrantAccess.all;
                            } 
                            else
                            {
                                itemDimension.access = SchemaRoleSchemaGrantCubeGrantDimensionGrantAccess.none;
                            }
                            countCheck++;
                        }

                        break;
                    }

                    if (!bResOk)
                    {
                        SchemaRoleSchemaGrantCubeGrantDimensionGrant newDimension = new SchemaRoleSchemaGrantCubeGrantDimensionGrant();
                        newDimension.dimension = entry.Key;
                        if(entry.Value.Equals("all"))
                        { 
                            newDimension.access = SchemaRoleSchemaGrantCubeGrantDimensionGrantAccess.all;
                        } 
                        else
                        {
                            newDimension.access = SchemaRoleSchemaGrantCubeGrantDimensionGrantAccess.none;
                        }
                        
                        listDimensionXMLROL.Add(newDimension);
                        countCheck++;
                    } 

                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkDimensionXMLROL, 3");
            }
            cubeXMLROL.DimensionGrant = listDimensionXMLROL.ToArray();
            listDimensionXMLROL.Clear();



            return countCheck;
        }

        // Проверка целостности иерархий в кубе
        private int checkHierarchyXMLROL(Schema objSchema, string roleName, ref SchemaRoleSchemaGrantCubeGrant cubeXMLROL)
        {

            int countCheck = 0;
            Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> dictObjAccessCubeXMLROL = dictObjAccessByRoleXMLROL[roleName];
            Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant> dictObjAccessHierarchyXMLROL = dictObjAccessCubeXMLROL[cubeXMLROL.cube];
            Dictionary<String, String> dictHierarchyXMLcb = null;

            try
            {

                if (dictDimensionByCubeXML.ContainsKey(cubeXMLROL.cube))
                {
                    dictHierarchyXMLcb = dictDimensionByCubeXML[cubeXMLROL.cube];
                }
                else if (dictDimensionByVirtualCubeXML.ContainsKey(cubeXMLROL.cube))
                {
                    dictHierarchyXMLcb = dictDimensionByVirtualCubeXML[cubeXMLROL.cube];
                }
                else
                {
                    dictHierarchyXMLcb = new Dictionary<String, String>();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkHierarchyXMLROL, 1");
            }

            Dictionary<String, Dictionary<String, bool>> dictHierarchyBase = null;
            Dictionary<String, bool> dictMemberBase = null;
            if (dictMemberByHierarhyByRoleXMLROL.ContainsKey(roleName))
            {
                dictHierarchyBase = dictMemberByHierarhyByRoleXMLROL[roleName];
            }

            //Проверка дименшенов - удаляем лишнее
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLROL = null;
            Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);

            try
            {

                if (cubeXMLROL.HierarchyGrant != null)
                {
                    listHierarchyXMLROL = cubeXMLROL.HierarchyGrant.ToList();
                    foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarchy in listHierarchyXMLROL)
                    {

                        if (!dictHierarchyXMLcb.ContainsKey(itemHierarchy.hierarchy))
                        { // по любому в dictHierarchyXMLcb иерархия обязана быть!
                            listHierarchyXMLROLforDelete.Add(itemHierarchy);
                        }
                        else
                        {
                            if (dctDimensionWithAnnotation.ContainsKey(itemHierarchy.hierarchy))  //Есть аннотация
                            {
                                bool flagCustom = false;
                                bool flagNoneAll = false;
                                SchemaRoleSchemaGrantCubeGrantHierarchyGrant resultHierarchyCustom = null;
                                //Проверяем, сколько таких (список одноименных дименшенов/иерархий)
                                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXML = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                                foreach (var resultHierarchy in listHierarchyXMLROL.Where(s => s.hierarchy == itemHierarchy.hierarchy))
                                {
                                    listHierarchyXML.Add(resultHierarchy);
                                    if(resultHierarchy.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                                    {
                                        if(flagCustom)
                                        {
                                            listHierarchyXMLROLforDelete.Add(resultHierarchy);
                                        }
                                        else
                                        {
                                            resultHierarchyCustom = resultHierarchy;
                                        }
                                        flagCustom = true;
                                    }
                                    else
                                    {
                                        if(flagNoneAll)
                                        {
                                            listHierarchyXMLROLforDelete.Add(resultHierarchy);
                                        }
                                        flagNoneAll = true;
                                    }
                                }

                                //Есть аннотация. Должен быть или только одна иерархия и она не custom
                                //Или одна custom и еще одна none или all

                                //Если один и custom
                                if (listHierarchyXML.Count == 1 && listHierarchyXML.ElementAt(0).access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                                {
                                    //Ecли один и custom, ставим ему доступ none и удаляем мемберы
                                    resultHierarchyCustom.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                                    if(resultHierarchyCustom.MemberGrant.Length>0){ 
                                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listMemberXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                                        resultHierarchyCustom.MemberGrant = listMemberXMLROLforDelete.ToArray();
                                    }
                                    countCheck++;
                                }
                            }
                            else
                            {
                                //Если у дименшена аннотации нет, но есть мемберы или тип custom, то удаляем
                                if (itemHierarchy.MemberGrant != null || itemHierarchy.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                                {
                                    listHierarchyXMLROLforDelete.Add(itemHierarchy);
                                }
                            }
                        }
                    }

                    foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarchyDel in listHierarchyXMLROLforDelete)
                    {
                        listHierarchyXMLROL.Remove(itemHierarchyDel);
                        countCheck++;
                    }

                }
                else
                {  // cubeXMLROL.HierarchyGrant == null, то создаем список иерархи1. Пока пустой.
                    listHierarchyXMLROL = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                }

                //Проверка иерархий - создаем недостающие
                foreach (KeyValuePair<String, String> entry in dictHierarchyXMLcb)
                {

                    SchemaRoleSchemaGrantCubeGrantHierarchyGrant objHierarchyXMLROL = null;

                    //Для всех новых иерархий доступ none
                    bool bResOk = false;
                    foreach (var result in listHierarchyXMLROL.Where(s => s.hierarchy == entry.Key))
                    {
                        bResOk = true;
                        break;
                    }

                    if (!bResOk)  //иерархии entry.Key в listHierarchyXMLROL нет. Добавляем с доступом none
                    {
                        SchemaRoleSchemaGrantCubeGrantHierarchyGrant newHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                        newHierarchy.hierarchy = entry.Key;
                        newHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                        newHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                        listHierarchyXMLROL.Add(newHierarchy);
                        countCheck++;
                    }


                    /*

                    if (dctDimensionWithAnnotation.ContainsKey(entry.Key))
                    {

                        //Проверяем, сколько таких (список одноименных дименшенов/иерархий)
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXML = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                        foreach (var resultHierarchy in listHierarchyXMLROL.Where(s => s.hierarchy == entry.Key))
                        {
                            listHierarchyXML.Add(resultHierarchy);
                        }
                        //Если один и none, значит забанен и дальше не проверяем
                        if (listHierarchyXML.Count == 1 && listHierarchyXML.ElementAt(0).access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none)
                        {
                            continue;
                        }

                        List<string> etalonHierarhyList = generateHierarchyKeyListByDictMemberHierarhy(roleName, entry.Key);
                        foreach (string et in etalonHierarhyList)
                        {
                            string[] itemEt = et.Split("##");
                            SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess etAccess = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                            if (itemEt[1] == "all")
                            {
                                etAccess = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all;
                            }
                            else if (itemEt[1] == "custom")
                            {
                                etAccess = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom;
                            }

                            bool bResOk = false;
                            //Ищем объект в listHierarchyXMLROL с такими же именем и доступом, как в эталоне
                            foreach (var result in listHierarchyXMLROL.Where(s => s.hierarchy == itemEt[0] && s.access == etAccess))
                            {
                                bResOk = true;
                                break;
                            }
                            // если не нашли, то добавляем
                            if (!bResOk)
                            {
                                SchemaRoleSchemaGrantCubeGrantHierarchyGrant newHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                                newHierarchy.hierarchy = itemEt[0];
                                newHierarchy.access = etAccess;
                                newHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                                listHierarchyXMLROL.Add(newHierarchy);
                                countCheck++;
                            }
                        }

                    }
                    else
                    {

                        bool bResOk = false;
                        foreach (var result in listHierarchyXMLROL.Where(s => s.hierarchy == entry.Key))
                        {
                            bResOk = true;
                            break;
                        }

                        if (!bResOk)  //иерархии entry.Key в listHierarchyXMLROL нет. Добавляем с доступом none
                        {
                            SchemaRoleSchemaGrantCubeGrantHierarchyGrant newHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                            newHierarchy.hierarchy = entry.Key;
                            newHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                            newHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                            listHierarchyXMLROL.Add(newHierarchy);
                            countCheck++;
                        }
                    }

                    */
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkHierarchyXMLROL, 2");
            }

            cubeXMLROL.HierarchyGrant = listHierarchyXMLROL.ToArray();
            listHierarchyXMLROL.Clear();

            try
            {

                for (int ind = 0; ind < cubeXMLROL.HierarchyGrant.Length; ind++)
                {
                    //Теперь мемберы не проверяем - не на что ориентироваться
                    /*

                    //Проверяем мемберы внутри иерархии (только те, у которых есть аннотации)
                    if (dctDimensionWithAnnotation.ContainsKey(cubeXMLROL.HierarchyGrant[ind].hierarchy))
                    {

                        //Проверяем только custom!!!
                        if (cubeXMLROL.HierarchyGrant[ind].access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                        {

                            countCheck += checkMembers(roleName, cubeXMLROL.HierarchyGrant[ind].hierarchy, ref cubeXMLROL.HierarchyGrant[ind]);
                        }
                        else
                        {

                            //Если не тип custom, то мемберов быть не должно!
                            if (cubeXMLROL.HierarchyGrant[ind].MemberGrant != null)
                            {
                                countCheck += cubeXMLROL.HierarchyGrant[ind].MemberGrant.Length;
                                cubeXMLROL.HierarchyGrant[ind].MemberGrant = null;
                            }
                        }
                    }
                    */
                    listHierarchyXMLROL.Add(cubeXMLROL.HierarchyGrant[ind]);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && checkHierarchyXMLROL, 3");
            }

            //Убираем Hierarchy с access=custom в конец списка
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLROLorder = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
            int checkOrder = 0; // режим отслеживания изменений
            foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant item in listHierarchyXMLROL)
            {
                if (item.access != SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                {
                    listHierarchyXMLROLorder.Add(item);
                    if (checkOrder == 1) checkOrder = 2;
                }
                else
                {
                    if (checkOrder == 0) checkOrder = 1;
                }
            }

            if (checkOrder == 2)
            { // Режим 2 - были изменения
                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant item in listHierarchyXMLROL)
                {
                    if (item.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                    {
                        listHierarchyXMLROLorder.Add(item);
                    }
                }

                cubeXMLROL.HierarchyGrant = listHierarchyXMLROLorder.ToArray();
                countCheck++;
            }

            return countCheck;
        }

        //Получить мапку дименшенов с аннотациями
        public Dictionary<String, String> getDictionaryDimensionWithAnnotation(Schema objSchema)
        {

            if (dictDimensionWithAnnotation != null) return dictDimensionWithAnnotation;

            dictDimensionWithAnnotation = new Dictionary<String, String>();

            foreach (SharedDimension item in objSchema.Dimension.ToList())
            {
                bool dimensionOff = false;
                if (item.Annotations != null && item.Annotations.Length > 0)
                { 
                    AnnotationsBase annotationsBase = item.Annotations[0];  
                    AnnotationBase annotationBase = annotationsBase.Annotation[0];
                    for (int ind = 0; ind < annotationBase.Annotation.Length; ind++){ 
                        AnnotationsAnnotation itemAnn = annotationBase.Annotation[ind];
                        if(itemAnn.name == "attributePermissionEnableAnnotation"){ 
                            //dictDimensionWithAnnotation.Add(item.name, itemAnn.Text[0]);
                            if(itemAnn.Text[0].Contains("AttributePermissionAnnotationOff"))
                            {
                                dimensionOff = true;
                                continue;
                            }
                        }
                    }
                }

                if(dimensionOff) continue;

                dictDimensionWithAnnotation.Add(item.name, item.name);
            }
            return dictDimensionWithAnnotation;
        }

        //Проверка мемберов. Функция устарела и не используется
        public int checkMembers(string roleName, String hierarchyName, ref SchemaRoleSchemaGrantCubeGrantHierarchyGrant objHierarchyXMLROL)
        {
            return 0;
            /*
            int countCheck = 0;
            //Список мемберов из XML
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listMemberXMLROL = null;

            if (objHierarchyXMLROL.MemberGrant != null)
            {
                listMemberXMLROL = objHierarchyXMLROL.MemberGrant.ToList();
            }
            else
            {
                listMemberXMLROL = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
            }

            //Список мемберов из основной мапки
            Dictionary<String, bool> dictMemberXMLROL = new Dictionary<string, bool>();
            Dictionary<String, bool> dictMemberBase = null;
            Dictionary<String, Dictionary<String, bool>> dictHierarchyBase = null;

            if (dictMemberByHierarhyByRoleXMLROL.ContainsKey(roleName))
            {
                dictHierarchyBase = dictMemberByHierarhyByRoleXMLROL[roleName];
            }

            if (dictHierarchyBase != null)
            {
                if (dictHierarchyBase.ContainsKey(hierarchyName))
                {
                    dictMemberBase = dictHierarchyBase[hierarchyName];
                }
                else
                {
                    dictMemberBase = new Dictionary<String, bool>();
                }
            }
            else
            {
                dictMemberBase = new Dictionary<String, bool>();
            }

            //Проверка мемберов - удаляем лишнее
            if (listMemberXMLROL.Count > 0)
            {

                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listMemberXMLROLforDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                //Готовим список для удаления
                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMemberXMLROL in listMemberXMLROL)
                {
                    if (!dictMemberBase.ContainsKey(itemMemberXMLROL.member))
                    {
                        listMemberXMLROLforDelete.Add(itemMemberXMLROL);
                    }
                }
                //Удаляем
                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMemberXMLROL in listMemberXMLROLforDelete)
                {
                    listMemberXMLROL.Remove(itemMemberXMLROL);
                    countCheck++;
                }

                objHierarchyXMLROL.MemberGrant = listMemberXMLROL.ToArray();
                listMemberXMLROL = objHierarchyXMLROL.MemberGrant.ToList();

                foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMemberXMLROL in listMemberXMLROL)
                {
                    if (itemMemberXMLROL.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all)
                    {
                        dictMemberXMLROL.Add(itemMemberXMLROL.member, true);
                    }
                    else
                    {
                        dictMemberXMLROL.Add(itemMemberXMLROL.member, false);
                    }
                }

            }


            //Добавляем
            foreach (KeyValuePair<String, bool> entry in dictMemberBase)
            {

                SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = null;

                if (!dictMemberXMLROL.ContainsKey(entry.Key))
                {

                    objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant();
                    objMember.member = entry.Key;

                    if (entry.Value == true)
                    {
                        objMember.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all;
                    }
                    else
                    {
                        objMember.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none;
                    }

                    listMemberXMLROL.Add((SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant)objMember);
                    dictMemberXMLROL.Add(objMember.member, entry.Value);
                    countCheck++;
                }
            }

            objHierarchyXMLROL.MemberGrant = listMemberXMLROL.ToArray();

            return countCheck;
            */
        }

        // Проверка целостности ролей
        public int checkRoleXML(bool bSave, ref Schema objSchema)
        {

            int countCheck = 0;
            List<SchemaRole> roleXmlForDelete = new List<SchemaRole>();

            //Список ролей из БД
            List<Role> listRole = getRoleList();
            Dictionary<String, Role> mapRoleBD = getMapRoleBD(listRole);

            //Список ролей из XML
            Dictionary<String, SchemaRole> mapRoleXML = getMapSchemaRole(objSchema);

            fillMemberDictionary(objSchema, bSave); //Заполняем основную мапку мемберов dictMemberByHierarhyByRoleXMLROL
            //Заполняем мапку реальными объектами доступа которые есть в схеме в секции Role
            countCheck += fillDictObjAccessByRoleXMLROL(objSchema);

            //Заполняем мапки кубов и виртуальных кубов, если они еще не заполнены
            filldictDimensionByCubeXML(objSchema);
            filldictDimensionByVirtualCubeXML(objSchema);

            //Удаляем лишние роли
            List<SchemaRole> lstShemRole = null;
            if (objSchema.Role == null)
            {
                lstShemRole = new List<SchemaRole>();
            }
            else
            {
                lstShemRole = objSchema.Role.ToList();
            }

            foreach (SchemaRole itemShemRole in lstShemRole)
            {
                if (!mapRoleBD.ContainsKey(itemShemRole.name))
                {
                    roleXmlForDelete.Add(itemShemRole);
                };
            }

            foreach (SchemaRole itemDel in roleXmlForDelete)
            {
                lstShemRole.Remove(itemDel);
                dictObjAccessByRoleXMLROL.Remove(itemDel.name);
                mapRoleXML.Remove(itemDel.name);
                countCheck++;
            }

            //Добавляем отсутствующие роли и контроль объектов доступа
            foreach (Role itemRole in listRole)
            {

                if (!dictObjAccessByRoleXMLROL.ContainsKey(itemRole.Name))
                {
                    //Создаем роль и объекты доступа заново
                    SchemaRole newSchemaRole = createNewSchemaRole(itemRole.Name);
                    lstShemRole.Add(newSchemaRole);
                    mapRoleXML.Add(itemRole.Name, newSchemaRole);
                    dictObjAccessByRoleXMLROL.Add(itemRole.Name, new Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>());  // ###
                    countCheck++;
                    countCheck += checkCubeXMLROL(objSchema, ref newSchemaRole);

                }
                else
                {
                    //Проверяем те, которые уже были
                    SchemaRole checkSchemaRole = mapRoleXML[itemRole.Name];
                    countCheck += checkCubeXMLROL(objSchema, ref checkSchemaRole);
                }

            }
            objSchema.Role = lstShemRole.ToArray();
            return countCheck;
        }


        //Создание эталона мемберов (на основе таблицы stationroles) (устаревшая функция)
        private List<string> generateHierarchyKeyListByDictMemberHierarhy(String role, String hierarchy)
        {

            List<string> listOut = new List<string>();

            if (!dictMemberByHierarhyByRoleXMLROL.ContainsKey(role))
            {
                listOut.Add(hierarchy + "##all");
                return listOut;
            }

            Dictionary<String, Dictionary<String, bool>> dictHierarchy = dictMemberByHierarhyByRoleXMLROL[role];
            if (!dictHierarchy.ContainsKey(hierarchy))
            { // если нет ни одной записи для данной иерархии, что-то пошло не так. Все запрещаем.
                listOut.Add(hierarchy + "##none");
                return listOut;
            }

            Dictionary<String, bool> dictMember = dictHierarchy[hierarchy];
            foreach (KeyValuePair<String, bool> entry in dictMember)
            {

                if (entry.Value == true)
                {
                    listOut.Add(hierarchy + "##none");
                }
                else
                {
                    listOut.Add(hierarchy + "##all");
                }

                if (entry.Key != "noname")
                {
                    listOut.Add(hierarchy + "##custom");
                }
                return listOut;
            }
            return listOut;
        }

        //Заполнение мемберов на основе таблицы stationroles из БД для вкладки "Роли по станциям"
        //Считаем данную функцию устаревшей
        public void fillMemberDictionary(Schema objSchema, bool withoutConditions)
        {
            dictMemberByHierarhyByRoleXMLROL = new Dictionary<String, Dictionary<String, Dictionary<String, bool>>>(); //####
            return;  //####
            /*
            string posError = "  && position: fillMemberDictionary, 0";

            //withoutConditions - выполнение в любом случае, даже если dictMemberByHierarhyByRoleXMLROL уже существует
            if (withoutConditions == false)
            {
                //Если основная мапка мемберов существует, считаем, что заполнять нечего и выходим.
                if (dictMemberByHierarhyByRoleXMLROL != null) return;
                dictMemberByHierarhyByRoleXMLROL = HttpContext.Session.GetObjectFromJson<Dictionary<String, Dictionary<String, Dictionary<String, bool>>>>("dictMemberByHierarhyByRoleXMLROLsession");
                if (dictMemberByHierarhyByRoleXMLROL != null) return;
            }

            try
            {
                //Удаление несуществующих ролей из stationroles
                string strsql = "delete from stationroles sd " +
                                "where  sd.id in ( " +
                                "    select str.id  " +
                                "    from stationroles str  " +
                                "    left outer join roles r on r.id = str.roleid  " +
                                "    where r.id is null " +
                                ")";

                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandText = strsql;
                    sql.CommandType = CommandType.Text;
                    sql.ExecuteNonQuery();
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
                posError = "  && position: fillMemberDictionary, 1";
                // Для каждой роли формируем массив "запрещенных" станций, т.е. станций, которые отсутствуют в таблице stationroles для данной роли
                // и массив "разрешенных" станций
                // в виде 233,278,317,324...
                string strsql2 = "select sg.roleid, sg.name as rolename, ss.stationlistban, sa.stationlistallowed " +
                                   " from ( select t.roleid, rl.name  from es.stationroles t " +
                                   "         inner join roles rl on rl.id = t.roleid " +
                                   "         group by t.roleid, rl.name " +
                                   "         ) sg, " +
                                   " lateral ( " +
                                   "     SELECT array_to_string(ARRAY( " +
                                   "         select st.id  " +
                                   "         from es.stations st " +
                                   "         left outer join (select * from es.stationroles where roleid = sg.roleid) str on str.stationid = st.id " +
                                   "         where str.stationid  is null " +
                                   "     ), ',') as stationlistban " +
                                   " ) ss, " +
                                   " lateral ( " +
                                   "     SELECT array_to_string(ARRAY( " +
                                   "         select st.id  " +
                                   "         from es.stations st " +
                                   "         inner join (select * from es.stationroles where roleid = sg.roleid) str on str.stationid = st.id " +
                                   "     ), ',') as stationlistallowed	 " +
                                   ") sa ";


                List<StationRoleOlapDTO> listStationRoleOlapDTO = new List<StationRoleOlapDTO>();
                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandText = strsql2;
                    sql.CommandType = CommandType.Text;
                    NpgsqlDataReader reader = sql.ExecuteReader();
                    while (reader.Read())
                    {
                        listStationRoleOlapDTO.Add(new StationRoleOlapDTO
                        {

                            RoleId = (long)reader["roleid"],
                            RoleName = (string)reader["rolename"],
                            StationListBan = (string)reader["stationlistban"],
                            StationListAllowed = (string)reader["stationlistallowed"]
                        });

                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }


                //Мапка Роль - Иерархия - Мембер
                dictMemberByHierarhyByRoleXMLROL = new Dictionary<String, Dictionary<String, Dictionary<String, bool>>>();
                //Список Мемберов
                int cnt = 0;
                foreach (StationRoleOlapDTO itemRole in listStationRoleOlapDTO)
                {
                    //Mапка Иерархия - общее число мемберо (станций)
                    Dictionary<String, long> dictToatalCountMember = new Dictionary<String, long>();
                    //Mапка Иерархия - общее число забаненых мемберов (станций)
                    Dictionary<String, long> dictBanCountMember = new Dictionary<String, long>();
                    StringBuilder sbSqlAnnotation = new StringBuilder("");
                    StringBuilder sbSqlAnnotationCountBan = new StringBuilder("");
                    StringBuilder sbSqlAnnotationCountTotal = new StringBuilder("");
                    //Заполняем мапку Иерархия - общее число мемберов (станций)
                    foreach (SharedDimension item in objSchema.Dimension.ToList())
                    {

                        if (item.Annotations != null && item.Annotations.Length > 0)
                        {
                            AnnotationsBase arrAnnTmp = null;
                            AnnotationsBase annotationsBase = item.Annotations[0];
                            AnnotationBase annotationBase = annotationsBase.Annotation[0];
                            AnnotationsAnnotation itemAnn = null;

                            for (int ind = 0; ind < annotationBase.Annotation.Length; ind++){ 
                                itemAnn = annotationBase.Annotation[ind];
                                if(itemAnn.name == "sqlAnnotation"){ 

                            // sql общего числа станций
                            if (sbSqlAnnotationCountTotal.Length > 1)
                            {
                                sbSqlAnnotationCountTotal.Append(" union all ");
                            }
                            String sqlAnnTotal = "select '" + item.name + "' as dimensionname, count(*) as counttotal from ( " + itemAnn.Text[0] + " ) d ";
                            sqlAnnTotal = sqlAnnTotal.Replace("?_operatoin_in_?", "not in");
                            sqlAnnTotal = sqlAnnTotal.Replace("?stationid?", "-100");
                            sbSqlAnnotationCountTotal.Append(sqlAnnTotal);

                                }
                            }
                           
                        }
                    }

                    if (sbSqlAnnotationCountTotal.ToString().Length > 2)
                    {
                        using (NpgsqlCommand sql = new NpgsqlCommand())
                        {
                            sql.Connection = _db.GetNpgsqlConnection();
                            sql.CommandText = sbSqlAnnotationCountTotal.ToString();
                            sql.CommandType = CommandType.Text;
                            posError = "  && position: fillMemberDictionary, 2, itemRole = '" + itemRole.RoleName + "'";
                            NpgsqlDataReader reader = sql.ExecuteReader();
                            while (reader.Read())
                            {
                                dictToatalCountMember.Add((string)reader["dimensionname"], (long)reader["counttotal"]);
                            }

                            sql.Connection.Close();
                            sql.Connection.Dispose();
                        }
                    }

                    //Заполняем мапку Иерархия - общее число забаненых мемберов (станций)
                    foreach (SharedDimension item in objSchema.Dimension.ToList())
                    {

                        if (item.Annotations != null && item.Annotations.Length > 0)
                        {
                            AnnotationsBase arrAnnTmp = null;
                            AnnotationsBase annotationsBase = item.Annotations[0];
                            AnnotationBase annotationBase = annotationsBase.Annotation[0];
                            AnnotationsAnnotation itemAnn = null;

                            for (int ind = 0; ind < annotationBase.Annotation.Length; ind++){ 
                                itemAnn = annotationBase.Annotation[ind];
                                if(itemAnn.name == "sqlAnnotation"){ 

                            // sql числа запрещенных станций
                            if (sbSqlAnnotationCountBan.Length > 1)
                            {
                                sbSqlAnnotationCountBan.Append(" union all ");
                            }
                            String sqlAnnBan = "select '" + item.name + "' as dimensionname, count(*) as countban from ( " + itemAnn.Text[0] + " ) d ";
                            sqlAnnBan = sqlAnnBan.Replace("?stationid?", itemRole.StationListBan);
                            sqlAnnBan = sqlAnnBan.Replace("?_operatoin_in_?", "in");
                            sbSqlAnnotationCountBan.Append(sqlAnnBan);

                                }
                            }
                        }
                    }

                    cnt++;
                    if(cnt == 23){ 
                        cnt = cnt;
                    }
                    //ИБ администраторы EMAS
                    if (sbSqlAnnotationCountBan.ToString().Length > 2)
                    {
                        using (NpgsqlCommand sql = new NpgsqlCommand())
                        {
                            sql.Connection = _db.GetNpgsqlConnection();
                            sql.CommandText = sbSqlAnnotationCountBan.ToString();
                            sql.CommandType = CommandType.Text;
                            posError = "  && position: fillMemberDictionary, 3, itemRole = '" + itemRole.RoleName + "'";
                            NpgsqlDataReader reader = sql.ExecuteReader();
                            while (reader.Read())
                            {
                                dictBanCountMember.Add((string)reader["dimensionname"], (long)reader["countban"]);

                            }

                            sql.Connection.Close();
                            sql.Connection.Dispose();
                        }
                    }

                    //Идем по всем кубам с аннотациями и получаем SQL-запросы для мемберов
                    foreach (SharedDimension item in objSchema.Dimension.ToList())
                    {
                        posError = "  && position: fillMemberDictionary, 4, itemRole = '" + itemRole.RoleName + "'";

                        if (item.Annotations != null && item.Annotations.Length > 0)
                        {
                            AnnotationsBase arrAnnTmp = null;
                            AnnotationsBase annotationsBase = item.Annotations[0];
                            AnnotationBase annotationBase = annotationsBase.Annotation[0];
                            AnnotationsAnnotation itemAnn = annotationBase.Annotation[0];

                            long countTotal = 0;
                            long countBan = 0;
                            if (dictToatalCountMember.ContainsKey(item.name))
                            {
                                countTotal = dictToatalCountMember[item.name];
                            }
                            if (dictBanCountMember.ContainsKey(item.name))
                            {
                                countBan = dictBanCountMember[item.name];
                            }

                            if (itemAnn.name == "sqlAnnotation")
                            {

                                if (sbSqlAnnotation.Length > 1)
                                {
                                    sbSqlAnnotation.Append(" union all ");
                                }

                                String sqlAnn = itemAnn.Text[0];

                                if (countTotal / 2 <= countBan)
                                {
                                    sqlAnn = sqlAnn.Replace("?stationid?", itemRole.StationListAllowed); // ban passive
                                }
                                else
                                {
                                    sqlAnn = sqlAnn.Replace("?stationid?", itemRole.StationListBan); // ban active
                                }
                                sqlAnn = sqlAnn.Replace("?_operatoin_in_?", "in");

                                sbSqlAnnotation.Append(sqlAnn);

                            }

                        }

                    }

                    if (sbSqlAnnotation.Length < 1) continue;

                    List<DimensionMemberDTO> listDimensionMemberDTO = new List<DimensionMemberDTO>();
                    using (NpgsqlCommand sql = new NpgsqlCommand())
                    {
                        sql.Connection = _db.GetNpgsqlConnection();
                        sql.CommandText = sbSqlAnnotation.ToString();
                        sql.CommandType = CommandType.Text;
                        posError = "  && position: fillMemberDictionary, 5, itemRole = '" + itemRole.RoleName + "'";
                        NpgsqlDataReader reader = sql.ExecuteReader();

                        while (reader.Read())
                        {
                            string hierarhyKey = (string)reader["dimensionname"];
                            string strDimensionmember = (string)reader["dimensionmember"];
                            long countTotal = 0;
                            long countBan = 0;
                            bool bAccessInMember = false;
                            if (itemRole.StationListAllowed.Length == 0)
                            { //Для роли ни один флаг не выставлен => все разрешено
                                bAccessInMember = false;
                                strDimensionmember = "noname";
                            }
                            else
                            {
                                if (dictToatalCountMember.ContainsKey(hierarhyKey))
                                {
                                    countTotal = dictToatalCountMember[hierarhyKey];
                                }
                                if (dictBanCountMember.ContainsKey(hierarhyKey))
                                {
                                    countBan = dictBanCountMember[hierarhyKey];
                                }
                                if (countTotal / 2 <= countBan)
                                {
                                    bAccessInMember = true;
                                }

                                if (countTotal == countBan)
                                { // все забанено
                                    bAccessInMember = true;
                                    strDimensionmember = "noname";
                                }

                            }

                            listDimensionMemberDTO.Add(new DimensionMemberDTO
                            {
                                DimensionName = hierarhyKey,
                                DimensionMember = strDimensionmember,
                                AccessInMember = bAccessInMember
                            });
                        }
                        sql.Connection.Close();
                        sql.Connection.Dispose();
                    }

                    posError = "  && position: fillMemberDictionary, 6";
                    if (listDimensionMemberDTO.Count < 1) continue;

                    Dictionary<String, Dictionary<String, bool>> dictMemberByHierarhy = null;
                    if (dictMemberByHierarhyByRoleXMLROL.ContainsKey(itemRole.RoleName))
                    {
                        dictMemberByHierarhy = dictMemberByHierarhyByRoleXMLROL[itemRole.RoleName];
                    }
                    else
                    {
                        dictMemberByHierarhy = new Dictionary<String, Dictionary<String, bool>>();
                        dictMemberByHierarhyByRoleXMLROL.Add(itemRole.RoleName, dictMemberByHierarhy);
                    }


                    foreach (DimensionMemberDTO itemDimensionMemberDTO in listDimensionMemberDTO)
                    {

                        //Проверка на вложенные скобки
                        var matchesSquareBracket = Regex.Matches(itemDimensionMemberDTO.DimensionMember, @"[[\]]+").Cast<Match>().Select(i => i.Value).ToArray();
                        string textoutSquareBracket = string.Join("", matchesSquareBracket);
                        var matchesSquareBracket2 = Regex.Matches(textoutSquareBracket, @"(.)\1+");
                        if (matchesSquareBracket2.Count > 0)
                        {
                            continue;
                        }

                        Dictionary<String, bool> dictMember = null;
                        if (dictMemberByHierarhy.ContainsKey(itemDimensionMemberDTO.DimensionName))
                        {
                            dictMember = dictMemberByHierarhy[itemDimensionMemberDTO.DimensionName];
                        }
                        else
                        {
                            dictMember = new Dictionary<String, bool>();
                            dictMemberByHierarhy.Add(itemDimensionMemberDTO.DimensionName, dictMember);
                        }

                        if (!dictMember.ContainsKey(itemDimensionMemberDTO.DimensionMember))
                        {
                            dictMember.Add(itemDimensionMemberDTO.DimensionMember, itemDimensionMemberDTO.AccessInMember);
                        }
                    }
                }
                posError = "  && position: fillMemberDictionary, 7";
                HttpContext.Session.SetObjectAsJson("dictMemberByHierarhyByRoleXMLROLsession", dictMemberByHierarhyByRoleXMLROL);

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }

            return;
            */
        }

        //сохранение схемы
        private void schemaSave(Schema objSchema)
        {

            //string filePath = getShemaFilePath(); ####
            string filePath = getShemaFilePathWithRoot();

            try
            {
                XmlSerializer serializer = new XmlSerializer(objSchema.GetType());
                using (XmlTextWriter tw = new XmlTextWriter(filePath, new UTF8Encoding(false)))
                {
                    tw.Formatting = System.Xml.Formatting.Indented;
                    serializer.Serialize(tw, objSchema);
                }

            }
            catch (Exception e)
            {
                throw new Exception(e.Message + "  && Ошибка при сохранении файла с OLAP-схемой, schemaSave, 1 ");
            }

            /*
            //Перезагрузка сервера после сохранения
            string username = null;
            string password = null;
            string url = null; 

            try
            {
                username   = getUserName_OlapServerRestart();
            }
            catch(Exception e)
            {
                throw new Exception("Не удалось получить имя пользователя для перезагрузки OLAP-сервера из таблицы настроек: " + e.Message + "  && schemaSave, 2");  
            }

            try
            {
                password   = getPassword_OlapServerRestart();
            }
            catch(Exception e)
            {
                throw new Exception("Не удалось получить пароль для перезагрузки OLAP-сервера из таблицы настроек: " + e.Message + "  && schemaSave, 3");  
            }

            try
            {
                url = getUrl_OlapServerRestart();
            }
            catch(Exception e)
            {
                throw new Exception("Не удалось получить URL для перезагрузки OLAP-сервера из таблицы настроек: " + e.Message + "  && schemaSave, 4");  
            }
            
            string encoded = System.Convert.ToBase64String(Encoding.GetEncoding("ISO-8859-1")
                                           .GetBytes(username + ":" + password));
            
            using var client = new HttpClient();
            var msg = new HttpRequestMessage(HttpMethod.Get, url);
            msg.Headers.Add("Authorization", "Basic " + encoded);

            try
            {
                var res =  client.Send(msg);
                var status = res.StatusCode;
                if(res.StatusCode == System.Net.HttpStatusCode.OK)
                { 
                    return;
                } 
                else 
                { 
                    throw new Exception("Не удалось перезагрузить сервер с мондрианом"  + "  && schemaSave, 5");
                }
            } 
            catch(Exception e)
            { 
                throw new Exception("Не удалось перезагрузить сервер с мондрианом: " + e.Message  + "  && schemaSave, 6");    
            }
            */
        }

        //Сохранение подготовленных изменений (если есть) доступа кубов. Возвращает схему с учетом изменений
        public Schema savePermissionChangesForCubes()
        {
            String posError = "  && savePermissionChangesForCubes, 0";
            String storageKey = null;
            Schema objSchema = null;
            int countCheck = 0;
            try
            {
                objSchema = getShema();
                if(objSchema == null) return null;
                countCheck = checkRoleXML(false, ref objSchema);
                storageKey = "PermissionOLAPCubeDataXML_" + HttpContext.Session.GetObjectFromJson<string>("CubeOlapAccessGUID");

                posError = "  && savePermissionChangesForCubes, 1";
                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    if (countCheck > 0)
                    {
                        schemaSave(objSchema);
                    }
                    return objSchema;
                }
                posError = "  && savePermissionChangesForCubes, 2";
                Dictionary<string, string> dictPermissionCubeData = PermissionDataStorage[storageKey];

                if (dictPermissionCubeData == null) return objSchema;
                Dictionary<String, SchemaRole> mapRole = getMapSchemaRole(objSchema);
                Dictionary<String, Dictionary<String, bool>> dictRoleChange = new Dictionary<String, Dictionary<String, bool>>();

                String cubeNameForSave = null;
                String roleKeyForSave = null;
                bool valueForSave = false;
                Dictionary<string, object> dictValeForSave = null;
                Dictionary<String, bool> distValue = null;

                posError = "  && savePermissionChangesForCubes, 3";
                //Разворачиваем в мапку по ролям
                foreach (KeyValuePair<string, string> entry in dictPermissionCubeData)
                {
                    cubeNameForSave = entry.Key;
                    dictValeForSave = JsonConvert.DeserializeObject<Dictionary<string, object>>(entry.Value);
                    foreach (KeyValuePair<string, object> item in dictValeForSave)
                    {
                        roleKeyForSave = item.Key;
                        valueForSave = (bool)item.Value;

                        if (!dictRoleChange.ContainsKey(roleKeyForSave))
                        {
                            distValue = new Dictionary<String, bool>();
                            dictRoleChange.Add(roleKeyForSave, distValue);
                        }
                        else
                        {
                            distValue = dictRoleChange[roleKeyForSave];
                        }
                        if (!distValue.ContainsKey(cubeNameForSave))
                        {
                            distValue.Add(cubeNameForSave, valueForSave);
                        }
                    }

                }

                posError = "  && savePermissionChangesForCubes, 4";
                //Мапка ключей и наименований ролей
                Dictionary<string, string> distKeyRole = new Dictionary<string, string>();
                List<Role> listRole = getRoleList();
                foreach (Role item in listRole)
                {
                    distKeyRole.Add(roleIdGenerate(item.Name), item.Name);
                }

                posError = "  && savePermissionChangesForCubes, 5";
                //Пробегаемся по мапке по ролям и модифицируем SchemaRole
                foreach (KeyValuePair<string, Dictionary<String, bool>> itemRoleChange in dictRoleChange)
                {

                    if (!distKeyRole.ContainsKey(itemRoleChange.Key)) { continue; }
                    string exchangeRole = distKeyRole[itemRoleChange.Key];

                    if (!mapRole.ContainsKey(exchangeRole)) { continue; }

                    distValue = itemRoleChange.Value;
                    SchemaRole objSchemaRole = mapRole[exchangeRole];
                    List<SchemaRoleSchemaGrantCubeGrant> lstCube = objSchemaRole.SchemaGrant[0].CubeGrant.ToList();

                    foreach (SchemaRoleSchemaGrantCubeGrant itemCubeGrant in lstCube)
                    {

                        if (!distValue.ContainsKey(itemCubeGrant.cube)) continue;
                        bool? val = distValue[itemCubeGrant.cube];
                        if (val == null) val = false;

                        if (val == true)
                        {
                            itemCubeGrant.access = SchemaRoleSchemaGrantCubeGrantAccess.all;
                        }
                        else
                        {
                            itemCubeGrant.access = SchemaRoleSchemaGrantCubeGrantAccess.none;
                        }

                    }
                    objSchemaRole.SchemaGrant[0].CubeGrant = lstCube.ToArray();
                }

                posError = "  && savePermissionChangesForCubes, 6";
                //Сохраняем
                schemaSave(objSchema);

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            finally
            {

                if (storageKey != null)
                {
                    PermissionDataStorage.Remove(storageKey);
                }
            }
            return objSchema;
        }
        //Сохранение подготовленных изменений (если есть) доступа дименшенов. Возвращает схему с учетом изменений
        public Schema savePermissionChangesForDimensions()
        {
            String posError = "  && position: savePermissionChangesForDimensions, 0";
            String storageKey = null;
            Schema objSchema = null;
            int countCheck = 0;
            try
            {
                objSchema = getShema();
                if(objSchema == null) return null;
                countCheck = checkRoleXML(false, ref objSchema);
                storageKey = "PermissionOLAPDimensionsDataXML_" + HttpContext.Session.GetObjectFromJson<string>("DimensionOlapAccessGUID");

                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    if (countCheck > 0)
                    {
                        schemaSave(objSchema);
                    }
                    return objSchema;
                }

                posError = "  && position: savePermissionChangesForDimensions, 1";
                Dictionary<string, string> dictPermissionDimensionData = PermissionDataStorage[storageKey];

                if (dictPermissionDimensionData == null)
                {
                    schemaSave(objSchema);
                    return objSchema;
                }
                Dictionary<String, SchemaRole> mapRole = getMapSchemaRole(objSchema);
                Dictionary<String, Dictionary<String, bool>> dictRoleChange = new Dictionary<String, Dictionary<String, bool>>();

                String dimensionNameForSave = null;
                String roleKeyForSave = null;
                bool valueForSave = false;
                Dictionary<string, object> dictValeForSave = null;
                Dictionary<String, bool> distValue = null;

                posError = "  && position: savePermissionChangesForDimensions, 2";
                //Разворачиваем в мапку по ролям dictRoleChange: [роль , [дименшн, значение]]
                foreach (KeyValuePair<string, string> entry in dictPermissionDimensionData)
                {
                    dimensionNameForSave = entry.Key;
                    dictValeForSave = JsonConvert.DeserializeObject<Dictionary<string, object>>(entry.Value);
                    foreach (KeyValuePair<string, object> item in dictValeForSave)
                    {
                        roleKeyForSave = item.Key;
                        valueForSave = (bool)item.Value;

                        if (!dictRoleChange.ContainsKey(roleKeyForSave))
                        {
                            distValue = new Dictionary<String, bool>();
                            dictRoleChange.Add(roleKeyForSave, distValue);
                        }
                        else
                        {
                            distValue = dictRoleChange[roleKeyForSave];
                        }
                        if (!distValue.ContainsKey(dimensionNameForSave))
                        {
                            distValue.Add(dimensionNameForSave, valueForSave);
                        }
                    }
                }

                posError = "  && position: savePermissionChangesForDimensions, 3";
                //Мапка ключей и наименований ролей
                Dictionary<string, string> distKeyRole = new Dictionary<string, string>();
                List<Role> listRole = getRoleList();
                foreach (Role item in listRole)
                {
                    distKeyRole.Add(roleIdGenerate(item.Name), item.Name);
                }

                Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);
                posError = "  && position: savePermissionChangesForDimensions, 4";
                //Пробегаемся по мапке по ролям и модифицируем SchemaRole
                foreach (KeyValuePair<string, Dictionary<String, bool>> itemRoleChange in dictRoleChange)
                {

                    if (!distKeyRole.ContainsKey(itemRoleChange.Key)) { continue; }
                    string exchangeRole = distKeyRole[itemRoleChange.Key];

                    if (!mapRole.ContainsKey(exchangeRole)) { continue; }

                    distValue = itemRoleChange.Value;
                    SchemaRole objSchemaRole = mapRole[exchangeRole];

                    if (objSchemaRole.SchemaGrant == null) continue;
                    if (objSchemaRole.SchemaGrant[0].CubeGrant == null) continue;

                    List<SchemaRoleSchemaGrantCubeGrant> lstCube = objSchemaRole.SchemaGrant[0].CubeGrant.ToList();

                    foreach (SchemaRoleSchemaGrantCubeGrant itemCubeGrant in lstCube)
                    { //Идем по кубам нужной роли

                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> lstHierarchy = itemCubeGrant.HierarchyGrant.ToList();
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> lstHierarchyForAdd = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> lstHierarchyForDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                        Dictionary<string, string> controlName = new Dictionary<string, string>(); //Иерархии, которые уже проверены

                        foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarchy in lstHierarchy)
                        {

                            if (!distValue.ContainsKey(itemHierarchy.hierarchy)) continue;

                            if (controlName.ContainsKey(itemHierarchy.hierarchy)) continue;

                            controlName.Add(itemHierarchy.hierarchy, itemHierarchy.hierarchy);

                            //Нашли иерархию, которую нужно менять
                            //Новое значение:
                            bool val = distValue[itemHierarchy.hierarchy];

                            if (dctDimensionWithAnnotation.ContainsKey(itemHierarchy.hierarchy))
                            {

                                if (val == true)
                                {
                                    //Просто добавляем пустой custom
                                    SchemaRoleSchemaGrantCubeGrantHierarchyGrant hewHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                                    hewHierarchy.hierarchy = itemHierarchy.hierarchy;
                                    hewHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                                    hewHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom;
                                    lstHierarchyForAdd.Add(hewHierarchy);

                                }
                                else
                                {
                                    //Убираем custom, не custom ставим none
                                    int cnt = 0;
                                    foreach (SchemaRoleSchemaGrantCubeGrantHierarchyGrant resultHierarchy in lstHierarchy.Where(s => s.hierarchy == itemHierarchy.hierarchy))
                                    {
                                        if (cnt == 0)
                                        {
                                            resultHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                                            resultHierarchy.MemberGrant = null;
                                        }
                                        else
                                        {
                                            lstHierarchyForDelete.Add(resultHierarchy);
                                        }
                                        cnt = 1;
                                    }
                                }

                            }
                            else
                            {

                                if (val == true)
                                {
                                    itemHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all;
                                }
                                else
                                {
                                    itemHierarchy.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                                }
                            }

                        }

                        if (lstHierarchyForDelete.Count > 0)
                        {
                            lstHierarchy = lstHierarchy.Except(lstHierarchyForDelete).ToList();
                        }
                        if (lstHierarchyForAdd.Count > 0)
                        {
                            lstHierarchy = lstHierarchy.Concat(lstHierarchyForAdd).ToList();
                        }

                        itemCubeGrant.HierarchyGrant = lstHierarchy.ToArray();
                    }

                    objSchemaRole.SchemaGrant[0].CubeGrant = lstCube.ToArray();

                }
                posError = "  && position: savePermissionChangesForDimensions, 5";
                countCheck = checkRoleXML(true, ref objSchema);
                //Сохраняем
                schemaSave(objSchema);

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            finally
            {

                if (storageKey != null)
                {
                    PermissionDataStorage.Remove(storageKey);
                }
            }
            return objSchema;
        }

        //Сохранение подготовленных изменений (если есть) доступа ролей по станциям. Возвращает схему с учетом изменений
        //(устаревшая функция)
        public Schema savePermissionChangesForRoleStation(List<Role> listRole, List<Station> listStation)
        {
            String posError = "  && position: savePermissionChangesForRoleStation, 0";
            String storageKey = null;
            Schema objSchema = null;
            int countCheck = 0;
            try
            {
                objSchema = getShema();
                if(objSchema == null) return null;
                storageKey = "PermissionOLAPRoleStationDataXML_" + HttpContext.Session.GetObjectFromJson<string>("RoleStationOlapAccessGUID");

                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    countCheck = checkRoleXML(false, ref objSchema);
                    if (countCheck > 0)
                    {
                        schemaSave(objSchema);
                    }
                    return objSchema;
                }
                posError = "  && position: savePermissionChangesForRoleStation, 1";
                Dictionary<string, string> dictPermissionRoleStationData = PermissionDataStorage[storageKey];

                if (dictPermissionRoleStationData == null)
                {
                    countCheck = checkRoleXML(false, ref objSchema);
                    if (countCheck > 0)
                    {
                        schemaSave(objSchema);
                    }
                    return objSchema;
                }
                posError = "  && position: savePermissionChangesForRoleStation, 2";
                Dictionary<string, long> dictRole = new Dictionary<string, long>();

                foreach (Role item in listRole)
                {
                    dictRole.Add(roleIdGenerate(item.Name), item.Id);
                }
                long StationRoleId = _db.Stationroles.Max(p => p.Id);
                posError = "  && position: savePermissionChangesForRoleStation, 3";
                foreach (KeyValuePair<string, string> entry in dictPermissionRoleStationData)
                {
                    string stationName = entry.Key;
                    long stationId = 0;
                    Station fStation = listStation.FirstOrDefault(p => p.Name == stationName, null);
                    if (fStation == null) continue;
                    stationId = fStation.Id;

                    Dictionary<string, object> dictValeForSave = JsonConvert.DeserializeObject<Dictionary<string, object>>(entry.Value);
                    foreach (KeyValuePair<string, object> item in dictValeForSave)
                    {

                        string roleName = item.Key;
                        if (!dictRole.ContainsKey(roleName)) continue;
                        long roleId = dictRole[roleName];

                        bool valueForSave = (bool)item.Value;
                        Stationrole stationRoleEx = null;
                        try
                        {
                            stationRoleEx = _db.Stationroles.FirstOrDefault(p => p.Roleid == roleId && p.Stationid == stationId);
                        }
                        catch (Exception ex)
                        {
                            //Ничего не делаем, просто идем дальше.
                        }

                        if (valueForSave == false && stationRoleEx != null)
                        {
                            _db.Stationroles.Remove(stationRoleEx);
                        }
                        else if (valueForSave == true && stationRoleEx == null)
                        {
                            stationRoleEx = new Stationrole
                            {
                                Id = ++StationRoleId,
                                Roleid = roleId,
                                Stationid = stationId
                            };
                            _db.Stationroles.Add(stationRoleEx);
                        }

                    }
                }
                posError = "  && position: savePermissionChangesForRoleStation, 4";
                _db.SaveChanges();
                countCheck = checkRoleXML(true, ref objSchema);
                if (countCheck > 0)
                {
                    schemaSave(objSchema);
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            finally
            {

                if (storageKey != null)
                {
                    PermissionDataStorage.Remove(storageKey);
                }
            }
            return objSchema;
        }


        // Data for CubesPermission Table
        public DataTable GetCubeGridData()
        {
            String posError = "  && position: GetCubeGridData, 0";
            DataTable result = new DataTable();

            try
            {
                Schema objSchema = savePermissionChangesForCubes();
                if(objSchema == null) return result;
                Dictionary<string, DataRow> map = new Dictionary<string, DataRow>();
                long idIndex = 0;

                List<Role> listRole = getRoleList();
                result.Columns.Add("OlapName");

                foreach (Role item in listRole)
                {
                    result.Columns.Add(roleIdGenerate(item.Name), typeof(bool));
                }
                posError = "  && position: GetCubeGridData, 1";
                Dictionary<String, SchemaRole> mapRole = getMapSchemaRole(objSchema);
                List<SchemaRoleSchemaGrantCubeGrant> lstCube = objSchema.Role[0].SchemaGrant[0].CubeGrant.ToList();
                DataRow lineRow = null;
                posError = "  && position: GetCubeGridData, 2";
                foreach (SchemaRoleSchemaGrantCubeGrant itemCube in lstCube)
                {
                    if(dictCubeVisible.ContainsKey(itemCube.cube))
                    {
                        if(dictCubeVisible[itemCube.cube] == false) continue;
                    }
                    else continue;

                    lineRow = result.NewRow();
                    lineRow["OlapName"] = itemCube.cube;
                    Boolean val = new Boolean();
                    

                    foreach (Role item in listRole)
                    {
                        val = false;
                        if (mapRole.ContainsKey(item.Name))
                        {

                            SchemaRole roleXML = mapRole[item.Name];
                            if (roleXML == null) break;

                            if (roleXML.SchemaGrant[0].CubeGrant != null)
                            {
                                foreach (SchemaRoleSchemaGrantCubeGrant itemCubeXML in roleXML.SchemaGrant[0].CubeGrant)
                                {
                                    if (itemCubeXML.cube.Equals(itemCube.cube))
                                    {

                                        if (itemCubeXML.access == SchemaRoleSchemaGrantCubeGrantAccess.all)
                                        {
                                            val = true;
                                        }
                                        else
                                        {
                                            val = false;
                                        }
                                    }

                                }
                            }
                            else
                            {
                                val = false;
                            }
                        }
                        lineRow[roleIdGenerate(item.Name)] = val;

                    }
                    result.Rows.Add(lineRow);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }

            return result;
        }

        // Data for DimensionsPermission Table
        public DataTable GetDimensionGridData()
        {
            String posError = "  && position: GetDimensionGridData, 0";
            DataTable result = new DataTable();

            try
            {
                Schema objSchema = savePermissionChangesForDimensions();
                if(objSchema == null) return result;
                Dictionary<string, DataRow> map = new Dictionary<string, DataRow>();
                long idIndex = 0;
                Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);

                List<Role> listRole = getRoleList();
                result.Columns.Add("OlapName");

                foreach (Role item in listRole)
                {
                    result.Columns.Add(roleIdGenerate(item.Name), typeof(bool));
                }
                posError = "  && position: GetDimensionGridData, 1";
                Dictionary<String, SchemaRole> mapRole = getMapSchemaRole(objSchema);

                SharedDimension[] arrDimension = objSchema.Dimension;
                if (arrDimension == null) return result;
                List<SharedDimension> lstDimension = objSchema.Dimension.ToList();
                posError = "  && position: GetDimensionGridData, 2";
                DataRow lineRow = null;
                foreach (SharedDimension itemDimension in lstDimension)
                {
                    lineRow = result.NewRow();
                    lineRow["OlapName"] = itemDimension.name;
                    bool val = false;

                    foreach (Role item in listRole)
                    {
                        val = false;
                        if (mapRole.ContainsKey(item.Name))
                        {
                            bool flBreak = false;
                            SchemaRole roleXML = mapRole[item.Name];

                            if (roleXML.SchemaGrant == null) continue;
                            if (roleXML.SchemaGrant[0].CubeGrant == null) continue;

                            foreach (SchemaRoleSchemaGrantCubeGrant itemCubeXMLROL in roleXML.SchemaGrant[0].CubeGrant)
                            {

                                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXML = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                                foreach (var resultHierarchy in itemCubeXMLROL.HierarchyGrant.ToList().Where(s => s.hierarchy == itemDimension.name))
                                {
                                    listHierarchyXML.Add(resultHierarchy);
                                }

                                if (listHierarchyXML.Count == 0) { continue; }

                                if (dctDimensionWithAnnotation.ContainsKey(itemDimension.name))
                                {

                                    //Если только один элемент и none, значит запрещен
                                    //В любом другом случае - разрешен
                                    if (listHierarchyXML.Count == 1 && listHierarchyXML.ElementAt(0).access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none)
                                    {
                                        val = false;
                                    }
                                    else
                                    {
                                        val = true;
                                    }

                                }
                                else
                                {

                                    if (listHierarchyXML.ElementAt(0).access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all)
                                    {
                                        val = true;
                                    }
                                    else
                                    {
                                        val = false;
                                    }
                                }
                                break;
                            }
                        }
                        lineRow[roleIdGenerate(item.Name)] = val;
                    }
                    result.Rows.Add(lineRow);
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            return result;
        }
        /*
        // УСТАРЕВШИЕ ФУНКЦИИ
        public DataTable GetRolestationGridData()
        {
            String posError = "  && position: GetRolestationGridData, 0";
            DataTable result = new DataTable();

            try
            {
                List<Role> listRole = getRoleList();
                List<Station> listStation = getStationList();
                Schema objSchema = savePermissionChangesForRoleStation(listRole, listStation);
                if(objSchema == null) return result;

                Dictionary<string, Dictionary<string, string>> objStationRole = getStationRoleDict();

                Dictionary<string, DataRow> map = new Dictionary<string, DataRow>();
                long idIndex = 0;
                result.Columns.Add("OlapName");

                foreach (Role item in listRole)
                {
                    result.Columns.Add(roleIdGenerate(item.Name), typeof(bool));
                }

                posError = "  && position: GetRolestationGridData, 1";
                DataRow lineRow = null;
                foreach (Station itemStation in listStation)
                {
                    lineRow = result.NewRow();
                    lineRow["OlapName"] = itemStation.Name;
                    Dictionary<string, string> dictRolesByStationRoles = null;
                    if (objStationRole.ContainsKey(itemStation.Name))
                    {
                        dictRolesByStationRoles = objStationRole[itemStation.Name];
                    }

                    foreach (Role item in listRole)
                    {
                        String roleId = roleIdGenerate(item.Name);
                        lineRow[roleId] = false;
                        if (dictRolesByStationRoles == null) continue;
                        if (!dictRolesByStationRoles.ContainsKey(item.Name)) continue;
                        lineRow[roleId] = true;
                    }
                    result.Rows.Add(lineRow);
                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }

            return result;

        }
        */
        protected IEnumerable<Dictionary<string, object>> ToDataTableList(DataTable table)
        {
            string[] columns = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
            IEnumerable<Dictionary<string, object>> result = table.Rows.Cast<DataRow>()
                    .Select(dr => columns.ToDictionary(c => c, c => dr[c]));
            return result;
        }

        static object lockerCube = new object();
        //Подготовка к сохранению доступа кубов
        public IActionResult saveOlapPermissionsCube(string key, string values)
        {
            bool lockWasTaken = false;
            Monitor.Enter(lockerCube, ref lockWasTaken);
            try
            {
                String storageKey = "PermissionOLAPCubeDataXML_" + HttpContext.Session.GetObjectFromJson<string>("CubeOlapAccessGUID");

                Dictionary<string, string> dictPermissionCubeData = null;
                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    dictPermissionCubeData = new Dictionary<string, string>();
                    PermissionDataStorage.Add(storageKey, dictPermissionCubeData);
                }
                else
                {
                    dictPermissionCubeData = PermissionDataStorage[storageKey];
                }

                if (dictPermissionCubeData.ContainsKey(key))
                {
                    dictPermissionCubeData[key] = values;
                }
                else
                {
                    dictPermissionCubeData.Add(key, values);
                }

            }
            finally
            {
                if (lockWasTaken)
                {
                    Monitor.Exit(lockerCube);
                }
            }
            return Ok();

        }

        static object lockerDimension = new object();
        //Подготовка к сохранению доступа дименшенов
        public IActionResult saveOlapPermissionsDimension(string key, string values)
        {
            bool lockWasTaken = false;
            Monitor.Enter(lockerDimension, ref lockWasTaken);
            try
            {
                String storageKey = "PermissionOLAPDimensionsDataXML_" + HttpContext.Session.GetObjectFromJson<string>("DimensionOlapAccessGUID");

                Dictionary<string, string> dictPermissionPermissionsData = null;
                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    dictPermissionPermissionsData = new Dictionary<string, string>();
                    PermissionDataStorage.Add(storageKey, dictPermissionPermissionsData);
                }
                else
                {
                    dictPermissionPermissionsData = PermissionDataStorage[storageKey];
                }

                if (dictPermissionPermissionsData.ContainsKey(key))
                {
                    dictPermissionPermissionsData[key] = values;
                }
                else
                {
                    dictPermissionPermissionsData.Add(key, values);
                }

            }
            finally
            {
                if (lockWasTaken)
                {
                    Monitor.Exit(lockerDimension);
                }
            }
            return Ok();
        }


        static object lockerRoleStation = new object();
        //Подготовка к сохранению доступа ролей по станции (устаревшая функция)
        public IActionResult saveOlapPermissionsRoleStation(string key, string values)
        {
            bool lockWasTaken = false;
            Monitor.Enter(lockerRoleStation, ref lockWasTaken);
            try
            {
                String storageKey = "PermissionOLAPRoleStationDataXML_" + HttpContext.Session.GetObjectFromJson<string>("RoleStationOlapAccessGUID");

                Dictionary<string, string> dictPermissionPermissionsData = null;
                if (!PermissionDataStorage.ContainsKey(storageKey))
                {
                    dictPermissionPermissionsData = new Dictionary<string, string>();
                    PermissionDataStorage.Add(storageKey, dictPermissionPermissionsData);
                }
                else
                {
                    dictPermissionPermissionsData = PermissionDataStorage[storageKey];
                }

                if (dictPermissionPermissionsData.ContainsKey(key))
                {
                    dictPermissionPermissionsData[key] = values;
                }
                else
                {
                    dictPermissionPermissionsData.Add(key, values);
                }

            }
            finally
            {
                if (lockWasTaken)
                {
                    Monitor.Exit(lockerRoleStation);
                }
            }
            return Ok();
        }

        public void btnRefreshRole_Click(string values)
        {
            var dh = new DataHelper(_configuration["ConnectionStrings:EmasConnection"]);
            var adConnectionString = dh.GetStringValueByProcedure("GetADConnString");
            var ad = new ActiveDirectory(adConnectionString, _db, _configuration);
            var dt = ad.GetGroups();
            dh.SaveDataByUdt("MergeADRoles", dt);

        }

        //Копирование роли
        public void CopyRole(long o_roleId, long n_roleId)
        {

            var userId = HttpContext.Session.GetObjectFromJson<long>("UserID");

            using (NpgsqlConnection conn = _db.GetNpgsqlConnection())
            {
                using (var command = new NpgsqlCommand("copyrolerights", conn)
                {
                    CommandType = CommandType.StoredProcedure
                })
                {

                    command.Parameters.AddWithValue("o_roleid", o_roleId);
                    command.Parameters.AddWithValue("n_roleid", n_roleId);
                    command.Parameters.AddWithValue("v_userid", userId);


                    if (command.Connection.State == ConnectionState.Closed)
                        command.Connection.Open();

                    command.ExecuteNonQuery();

                    if (command.Connection.State != ConnectionState.Closed)
                        command.Connection.Close();

                }
            }



        }

        //Копирование роли (в части прав доступа к OLAP объектам. Результат копирования сохраняется в OLAP-схеме)
        public string CopyRoleOlap(long src_roleId, long dst_roleId)
        { 
            string srcRole = _db.Roles.Select(f => new RoleDTO { Id = f.Id, Name = f.Name, Description = f.Description }).Where(f => f.Id == src_roleId).Select(f => f.Name).First();
            string dstRole = _db.Roles.Select(f => new RoleDTO { Id = f.Id, Name = f.Name, Description = f.Description }).Where(f => f.Id == dst_roleId).Select(f => f.Name).First();
            Schema objSchema = null;
            SchemaRole srcShemRole = null;
            SchemaRole dstShemRole = null;
            bool srcFlg=false;
            bool dstFlg=false;

            try
            {
                objSchema = getShema();
                if(objSchema == null)
                {
                    return "Ошибка. Не удалось загрузить OLAP-схему.";
                }

                foreach (SchemaRole itemShemRole in objSchema.Role.ToList()){ 
                    if(itemShemRole.name.Equals(srcRole) ){ 
                        srcShemRole =  itemShemRole;   
                        srcFlg = true;
                    }
                    if(itemShemRole.name.Equals(dstRole) ){ 
                        dstShemRole =  itemShemRole;   
                        dstFlg = true;
                    }
                    if(srcFlg && dstFlg) break;
                }
                if(!srcFlg){ 
                    return "Ошибка. Роль '" + srcRole + "' в OLAP-схеме не найдена";
                }
                if(!dstFlg){ 
                    return "Ошибка. Роль '" + dstRole + "' в OLAP-схеме не найдена";
                }

                copyRoleCubeAndHierarhyGrantAccess(srcShemRole, dstShemRole, objSchema);

            }
            catch (Exception ex)
            {
                return "Ошибка. " + ex.Message;
            }
            return "Скопировано";
        }

        //Копирование содержимого роли srcShemRole в роль dstShemRole
        private void copyRoleCubeAndHierarhyGrantAccess(SchemaRole srcShemRole, SchemaRole dstShemRole, Schema objSchema){ 

            fillMemberDictionary(objSchema, false);

            //Мапка доступа к кубам в роли источнике
            Dictionary<String, SchemaRoleSchemaGrantCubeGrantAccess> srcDictCubeGrantAccess = new Dictionary<String, SchemaRoleSchemaGrantCubeGrantAccess>();

            //Мапка прав доступа у иерархий в роли-источнике
            Dictionary<String, bool> srcDictDimensionGrantAccess = new Dictionary<String, bool>();

            //Мапка с кубами и иерархиями роли-источника
            //Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> srcDictCubeHierarhy = new Dictionary<String, Dictionary<String, SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>();
            Dictionary<String, List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>> srcDictCubeHierarhy = new Dictionary<String, List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>>();

            //Формируем мапки доступа кубов и иерархий у источника
            foreach(SchemaRoleSchemaGrantCubeGrant itemCube in srcShemRole.SchemaGrant[0].CubeGrant.ToList())
            {
                srcDictCubeGrantAccess.Add(itemCube.cube, itemCube.access);

                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> srcListHierarhy = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                srcDictCubeHierarhy.Add(itemCube.cube, srcListHierarhy);

                foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarchy in itemCube.HierarchyGrant.ToList())
                {
                    SchemaRoleSchemaGrantCubeGrantHierarchyGrant newHierarchy = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant();
                    newHierarchy.hierarchy = itemHierarchy.hierarchy;
                    newHierarchy.rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial;
                    newHierarchy.access = itemHierarchy.access;

                    if(itemHierarchy.MemberGrant!= null)
                    {
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> lstMemberGrantDest = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();

                        foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant srcItem in itemHierarchy.MemberGrant)
                        {
                            SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                                    {

                                        member = srcItem.member, 
                                        access = srcItem.access
                                    };
                            lstMemberGrantDest.Add(objMember);
                        }
                        newHierarchy.MemberGrant = lstMemberGrantDest.ToArray();
                    }

                    srcListHierarhy.Add(newHierarchy);
                }
            }

            //формируем доступ к кубам у роли назначения
            for(int ind = 0; ind<dstShemRole.SchemaGrant[0].CubeGrant.Length; ind++)
            {
                dstShemRole.SchemaGrant[0].CubeGrant[ind].access = srcDictCubeGrantAccess[dstShemRole.SchemaGrant[0].CubeGrant[ind].cube];
            }

            //формируем доступ к иерархиям у роли назначения
            //Идем по роли-назначения
            //Исходим из того, что
            //Набор кубов одинаковый
            //Набор иерархий в кубах одинаковый

            foreach(SchemaRoleSchemaGrantCubeGrant dstItemCube in dstShemRole.SchemaGrant[0].CubeGrant.ToList())
            {
                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> srcListHierarhy = srcDictCubeHierarhy[dstItemCube.cube];
                dstItemCube.HierarchyGrant = srcListHierarhy.ToArray();
            }
            //Теги DimensionGrant сфорируются при обновлении страницы

            //Сохраняем
            schemaSave(objSchema);

        }
 
        //========================== 1 ================================
        public object GetRoleDictionary(DataSourceLoadOptions loadOptions)
        {

            List<Role> model1 = new List<Role>();


            try
            {
                //model = _db.Roles.OrderBy(p => p.Name).ToList();
                

                Dictionary<long, string> model = _db.Roles.Select(input => input) //s => new{ s.Id, s.Value, s.Name } )
                     .Where(p => p.Name == "Администраторы EMAS" || p.Name == "Аналитики EMAS")  // ####
                .OrderBy(input => input.Name)
                .ToDictionary(x => (long)x.Id, x => x.Name)
                .OrderBy(i=>i.Value)
                .Select((entry, i) => new { entry.Value, i })
                .ToDictionary(pair=>(long)pair.i, pair=>pair.Value);

                return DataSourceLoader.Load(model, loadOptions);

            }
            catch (Exception ex)
            {
                throw new Exception("Ошибка загрузки таблицы roles");
            }
            return null;

        }

        public MondrianDataSources getMondrianDataSources()
        {
            MondrianDataSources objMondrianDataSources = null;

            string? filePathRoot = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAP_RootPatch")?.Value;
            if(filePathRoot == null)
            {
               throw new Exception("Не удалось получить из базы данных корневой путь к OLAP-файлам && getMondrianDataSources, 1"); 
            }

            string? filePath = _db.VSystemsettings.FirstOrDefault(s => s.Name == "OLAP_DataSources")?.Value;

            if(filePath == null)
            {
               throw new Exception("Не удалось получить из базы данных путь к файлу DataSource.xml  && getMondrianDataSources, 2"); 
            }

            String strDataSourceFile = "";
            filePath = filePathRoot + filePath;

            try
            {
                strDataSourceFile = XDocument.Load(filePath).ToString();
            }
            catch (Exception ex)
            {
                throw new Exception("Не найден файл " + filePath + "  && getMondrianDataSources, 3");
            }

            try
            {
                XmlRootAttribute xmlRoot = new XmlRootAttribute
                {
                    ElementName = "DataSources",
                    IsNullable = true
                };

                XmlSerializer serializer = new XmlSerializer(typeof(MondrianDataSources), xmlRoot);


                using (StringReader reader = new StringReader(strDataSourceFile))
                {
                    objMondrianDataSources = (MondrianDataSources)serializer.Deserialize(reader);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + "  && getMondrianDataSources, 4");
            }

            return objMondrianDataSources;

        }

        public Dictionary<long, string> GetShemaFilePathDict()
        {
            MondrianDataSources objMondrianDataSources = getMondrianDataSources();

            Dictionary<long, string> model = new Dictionary<long, string>();
            long cnt = 0;
 
            if(objMondrianDataSources != null)
            {
                foreach(DataSourcesDataSourceCatalog item in objMondrianDataSources.DataSource.Catalogs)
                {
                    string strDef = item.Definition;
                    model.Add(cnt++, strDef);
                }
            }

            return model;
        }
        //========================== 2 ================================










        // ++++++++++++++++++++++++++++++++++++++++++
        // ++++++++++++++++++++++++++++++++++++++++++
        // ++++++++++++++++++++++++++++++++++++++++++
        // ++++++++++++++++++++++++++++++++++++++++++
        // ++++++++++++++++++++++++++++++++++++++++++

        //Получить список дименшенов с аннотациями атрибутов (список слева на вкладке доступа к атрибутам)
        public object GetDimensionList(string shemafile, DataSourceLoadOptions loadOptions)
        {
            List<Items> model = new List<Items>();
            if(shemafile == null)
            {
                Dictionary<long, string> modelShemaFile = GetShemaFilePathDict();
                if(modelShemaFile == null || modelShemaFile.Count == 0)
                {
                    return DataSourceLoader.Load(model, loadOptions);
                }
                HttpContext.Session.SetObjectAsJson("ShemaFileSelected", modelShemaFile[0]);
            } 
            else
            {
                HttpContext.Session.SetObjectAsJson("ShemaFileSelected", shemafile);
            }

            Schema objSchema = getShema();
            if(objSchema == null)
            {
                return DataSourceLoader.Load(model, loadOptions);
            }

            int cnt = 0;
 
            Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);

            foreach (KeyValuePair<String, string> entry in dctDimensionWithAnnotation){ 
                Items item = new Items();
                item.Id = cnt++;
                item.Name = entry.Key;
                model.Add((Items)item);    
            }

            return DataSourceLoader.Load(model, loadOptions);
        }

        //Получить текст (шаблон sql запроса) аннотации атрибутов
        // Не используется
        /*
        string getAttributeDimensionSqlAnnotation(string dimensionName, Schema objSchema)
        { 
            Dictionary<String, String> dctDimensionWithAnnotation = getDictionaryDimensionWithAnnotation(objSchema);
            if(dctDimensionWithAnnotation.ContainsKey(dimensionName))
            {
                return dctDimensionWithAnnotation[dimensionName];
            } 
            else return null;

        }
        */
 
        // Выполнение запроса strsql в постгресе
        public List<TreeListNode> executeOlapQueryPostgre(string strsql, Schema objSchema, int itemId, string itemRoleName, int parentId, byte levelId, string roleName, string dimensionName,string memberPatch)
        {
            List<TreeListNode> model = new List<TreeListNode>();
            String posError = "  && position: executeOlapQueryPostgre, 1, itemRole = '" + itemRoleName + "'";

            try
            {
                using (NpgsqlCommand sql = new NpgsqlCommand())
                {
                    sql.Connection = _db.GetNpgsqlConnection();
                    sql.CommandText = strsql; //.ToString();
                    sql.CommandType = CommandType.Text;
                    posError = "  && position: executeOlapQueryPostgre, 2, itemRole = '" + itemRoleName + "' + Error ExecuteReader(). sql: " + strsql;
                    NpgsqlDataReader reader = sql.ExecuteReader();

                    //int itemId = maxIdAttribute;
                    while (reader.Read())
                    {
                        string hierarhyKey = (string)reader["dimensionname"];
                        string strObjectName = (string)reader["objectName"];

                        posError = "  && position: executeOlapQueryPostgre, 3, itemRole = '" + itemRoleName + "'  Error NpgsqlDataReader.Read. hierarhyKey = " + hierarhyKey + "  strObjectName = " + strObjectName + " sql: " + strsql;

                        //long countTotal = 0;
                        //long countBan = 0;
                        //bool bAccessInMember = false;
                        TreeListNode itemTree = new TreeListNode();

                        itemTree.IsExpanded = false; itemTree.IsSelected = true;  itemTree.Level = levelId;
                        itemTree.Id = ++itemId; itemTree.ParentId = (parentId<0?null:parentId); 
                        itemTree.NodeName = strObjectName;
                        string memberPatchFull = string.Format("{0}.[{1}]", memberPatch, strObjectName);

                        bool itemAccess = getMemberAccess(objSchema, roleName, dimensionName, memberPatchFull);
                        itemTree.selected = itemAccess;

                        model.Add((TreeListNode)itemTree);  
                            
                    }
                    sql.Connection.Close();
                    sql.Connection.Dispose();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }

            return model;
        }

        //Распарсивание значения jdbc из секции DataSourceInfo в dataSource.xml
        public void jdbcParse(string srcStr, ref Dictionary<String, String> dicProperty) 
        {
            String[] arrStr1 =  srcStr.Split('?');
            String[] arrStr2 = arrStr1[0].Split('/');
            for(int ind = 0; ind < arrStr2.Length; ind++)
            {
                int count = arrStr2[ind].Count(c => c == '.');
                if(count == 3){ 
                    String[] arrStr3 = arrStr2[ind].Split(':');
                    dicProperty.Add("Host",  arrStr3[0]);
                    if(arrStr2.Length>ind+1)
                    {
                        dicProperty.Add("Database",  arrStr2[ind+1]);
                    }
                }
            }
            
        }

        //Трансляция секции DataSourceInfo из dataSource.xml в справочник "ключ-значение" (только в случае работы через кликхаус)
        public Dictionary<String, String>  getPropertyByConnectionString(string srcStr)
        {
            Dictionary<String, String> dicProperty = new Dictionary<String, String>();

            String[] itemsGen = srcStr.Split('?');

            String[] items1 = itemsGen[0].Split(';');
            String[] items2 = itemsGen[1].Split(';');

            foreach(string item in items1)
            {
                String[] strProp = item.Split('=');

                if(strProp[0] == "Jdbc")
                { 
                    jdbcParse(strProp[1], ref dicProperty);

                }
                else
                {
                    dicProperty.Add(strProp[0], strProp[1]);
                }
            }

            foreach(string item in items2)
            {
                String[] strProp = item.Split('=');
                dicProperty.Add(strProp[0], strProp[1]);
            }

            return dicProperty;
        }
        
        // Выполнение запроса strsql в кликхаусе
        public List<TreeListNode> executeOlapQueryClickHouse(string strsql, Schema objSchema, int itemId, string itemRoleName, int parentId, byte levelId, string roleName, string dimensionName, string memberPatch)
        {
            string posError = "  && position: executeOlapQueryClickHouse, 0";
            List<TreeListNode> model = new List<TreeListNode>();
            ClickHouseConnection? conn = null;

            //Параметры подключения
            //String connectionString = HttpContext.Session.GetObjectFromJson<string>("ClickHouseConnectionString");
            ClickHouseConnectionStringBuilder connectionClickHouse = HttpContext.Session.GetObjectFromJson<ClickHouseConnectionStringBuilder>("ClickHouseConnection");

            String connectionString = null;
            if(connectionString == null)
            {
                MondrianDataSources dataSources = getMondrianDataSources();
                string strConnectInfo =  dataSources.DataSource.DataSourceInfo;
                Dictionary<String, String> dicProperty = getPropertyByConnectionString(strConnectInfo);

                connectionClickHouse = new ClickHouseConnectionStringBuilder();
                
                if(!dicProperty.ContainsKey("Host"))
                {
                    throw new Exception("  && position: executeOlapQueryClickHouse, create ConnectionStringBuilder, not 'Host' property");
                }
                else
                {
                    connectionClickHouse.Host = dicProperty["Host"];
                }

                if(!dicProperty.ContainsKey("PortTCP"))
                {
                    connectionClickHouse.Port = 9000; // 9000 - default port TCP, 8123 - default port HTTP
                }
                else
                {
                    connectionClickHouse.Port = ushort.Parse(dicProperty["PortTCP"]);
                }

                if(!dicProperty.ContainsKey("JdbcPassword"))
                {
                    throw new Exception("  && position: executeOlapQueryClickHouse, create ConnectionStringBuilder, not 'JdbcPassword' property");
                }
                else
                {
                    connectionClickHouse.Password = dicProperty["JdbcPassword"];
                }

                if(!dicProperty.ContainsKey("JdbcUser"))
                {
                    throw new Exception("  && position: executeOlapQueryClickHouse, create ConnectionStringBuilder, not 'JdbcUser' property");
                }
                else
                {
                    connectionClickHouse.User = dicProperty["JdbcUser"];
                }

                if(!dicProperty.ContainsKey("Database"))
                {
                    throw new Exception("  && position: executeOlapQueryClickHouse, create ConnectionStringBuilder, not 'Database' property");
                }
                else
                {
                    connectionClickHouse.Database = dicProperty["Database"];
                }

                if(!dicProperty.ContainsKey("socket_timeout"))
                {
                    throw new Exception("  && position: executeOlapQueryClickHouse, create ConnectionStringBuilder, not 'socket_timeout' property");
                }
                else
                {
                    connectionClickHouse.CommandTimeout = int.Parse(dicProperty["socket_timeout"]);
                }
                //socket_timeout
                //connectionClickHouse.CommandTimeout

                HttpContext.Session.SetObjectAsJson("ClickHouseConnection", connectionClickHouse);
            }

            try
            {
                posError = "  && position: executeOlapQueryClickHouse, 1";	

                conn = new ClickHouseConnection(connectionClickHouse);
                conn.Open();
                var res = "";
                var cmd = conn.CreateCommand(strsql);
                Octonica.ClickHouseClient.ClickHouseDataReader reader = cmd.ExecuteReader();
                int cntReader = 0;
                while(reader.Read())
                {
                    cntReader++;
                    posError = "  && position: executeOlapQueryClickHouse, 2";

                    string hierarhyKey = reader.GetString("dimensionname");
                    var vrObjectName = reader.GetValue("objectName");

                    if(reader.GetValue("objectName") is System.DBNull)
                    {
                        continue;
                    }

                    string strObjectName = vrObjectName.ToString();

                    posError = "  && position: executeOlapQueryClickHouse, 3, strObjectName = " + strObjectName + "  cntReader = " + cntReader;

                    TreeListNode itemTree = new TreeListNode();

	                itemTree.IsExpanded = false; itemTree.IsSelected = true;  itemTree.Level = levelId;
	                itemTree.Id = ++itemId; itemTree.ParentId = (parentId<0?null:parentId); 
	                itemTree.NodeName = strObjectName;
	                string memberPatchFull = string.Format("{0}.[{1}]", memberPatch, strObjectName);

	                bool itemAccess = getMemberAccess(objSchema, roleName, dimensionName, memberPatchFull);
	                itemTree.selected = itemAccess;

	                model.Add((TreeListNode)itemTree);  

                }

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }
            finally
            {
                if(conn!=null)
                {
                    try
                    {
                        conn.Close();
                    }
                    catch (Exception ex)
                    {
                        throw new Exception(ex.Message + posError);
                    }
                }
            }

            return model;
        }
        
        // Коррекция sql для postgre
        public string sqlForAccessAtributePostgre(string sql)
        {
            sql = sql.Replace("`", "");
            sql = sql.Replace("toString", "");
            return sql;
        }
        
        // создание sql запроса для кликхауса
        public string createSqlForAccessAtributeClickHouse( string dimensionName, Schema objSchema)
        {
            //string sql;
            string sqlStart;
            string sqlEnd;
            string srcSql = null;
            int sizeLevel = 0;
            string[] strLevel = null;

            foreach(SharedDimension itemD in objSchema.Dimension)
            { 
                if(itemD.name != dimensionName) continue;

                srcSql = itemD.Hierarchy[0].View.SQL[0].Text[0];
                HierarchyLevel[] hl = itemD.Hierarchy[0].Level;
                sizeLevel = hl.Length;
                strLevel = new string[sizeLevel];

                for(int ind = 0; ind<sizeLevel; ind++){
                    if (Regex.IsMatch(hl[ind].column, @"\p{IsCyrillic}"))  //Есть ли в тексте кирилица?
                    {
                        strLevel[ind] = string.Format("`{0}`", hl[ind].column);
                    }
                    else
                    {
                        strLevel[ind] = hl[ind].column;
                    }
                }
            }

            sqlStart = string.Format("select distinct '{0}' as dimensionname, case when 1=?treelevel? then toString(tsx.{1}) ", dimensionName, strLevel[0] );
            if(sizeLevel > 1)
            {
                sqlEnd = " ) tsx where ";
            }
            else
            {
                sqlEnd = string.Format(" ) tsx  where ?treelevel? <= {0} ", sizeLevel); // " ) tsx ";
            }

            for(int ind = 1; ind<sizeLevel; ind++)
            {
                sqlStart += string.Format("when {0}=?treelevel? then toString(tsx.{1}) ", ind+1, strLevel[ind] );

                if(ind == 1)
                {
                    sqlEnd += string.Format(" ((toString(tsx.{0}) = '?objectName{1}?'  and {2} <= ?treelevel?) or ({2} > ?treelevel?)) ", strLevel[ind-1], ind, ind+1);
                }
                else
                {
                    sqlEnd += string.Format(" and ((toString(tsx.{0}) = '?objectName{1}?'  and {2} <= ?treelevel?) or ({2} > ?treelevel?)) ", strLevel[ind-1], ind, ind+1);
                }
            }

            sqlStart += "else 'noname' end as objectName from ( ";
            if(sizeLevel > 1)
            {
                sqlEnd += string.Format("and ?treelevel? <= {0} ", sizeLevel);
            }

            return sqlStart + srcSql +  sqlEnd;
        }

        // создание sql запроса для постгреса
        public string createSqlForAccessAtributePostgresql( string dimensionName, Schema objSchema)
        {
            //string sql;
            string sqlStart;
            string sqlEnd;
            string srcSql = null;
            int sizeLevel = 0;
            string[] strLevel = null;

            foreach(SharedDimension itemD in objSchema.Dimension)
            { 
                if(itemD.name != dimensionName) continue;

                srcSql = itemD.Hierarchy[0].View.SQL[0].Text[0];
                HierarchyLevel[] hl = itemD.Hierarchy[0].Level;
                sizeLevel = hl.Length;
                strLevel = new string[sizeLevel];

                for(int ind = 0; ind<sizeLevel; ind++)
                {
                    strLevel[ind] = hl[ind].column;
                }
            }
            srcSql = sqlForAccessAtributePostgre(srcSql);

            sqlStart = string.Format("select distinct '{0}' as dimensionname, case when 1=?treelevel? then tsx.{1} ", dimensionName, strLevel[0] );
            if(sizeLevel > 1)
            {
                sqlEnd = " ) tsx where ";
            }
            else
            {
                sqlEnd = " ) tsx ";
            }

            for(int ind = 1; ind<sizeLevel; ind++)
            {
                sqlStart += string.Format("when {0}=?treelevel? then tsx.{1} ", ind+1, strLevel[ind] );

                if(ind == 1)
                {
                    sqlEnd += string.Format(" ((tsx.{0} = '?objectName{1}?'  and {2} <= ?treelevel?) or ({2} > ?treelevel?)) ", strLevel[ind-1], ind, ind+1);
                }
                else
                {
                    sqlEnd += string.Format(" and ((tsx.{0} = '?objectName{1}?'  and {2} <= ?treelevel?) or ({2} > ?treelevel?)) ", strLevel[ind-1], ind, ind+1);
                }
            }

            sqlStart += "else 'noname' end as objectName from ( ";
            if(sizeLevel > 1)
            {
                sqlEnd += string.Format("and ?treelevel? <= {0} ", sizeLevel);
            }

            return sqlStart + srcSql +  sqlEnd;
        }

        // ФЛАГ ВЫПОЛНЕНИЯ ЗАПРОСОВ ЧЕРЕЗ ПОСТГРЕС
        bool postgresEnable = false;

        //Список атрибутов для заданного уровня ( возвращает List<TreeListNode> )
        public object getListAttributeOlapTreeForLevel(int parentId, byte levelId, string dimensionName, string hierarhy_seq, int maxIdAttribute, string shemaFileName, string roleName)
        { 
            //List<TreeListNode> model = new List<TreeListNode>();
            string posError = "  && position: getListAttributeOlapTreeForLevel, 0";	

            try{
                HttpContext.Session.SetObjectAsJson("ShemaFileSelected", shemaFileName);
                string itemRoleName = roleName;
                string memberPatch = string.Format("[{0}]", dimensionName);
                //return string.Format("Id: {0}, Name: {1}", Id, Name);
                //[Станция - Договор].[ТЭЦ-30]

                Schema objSchema = getShema();
                if(objSchema == null)
                {
                    return new List<TreeListNode>();
                }
 
                int countCheck = checkRoleXML(false, ref objSchema);
                if (countCheck > 0)
                {
                    schemaSave(objSchema);
                }
                
                string strsql = null;

                if(postgresEnable)
                {
                    strsql = createSqlForAccessAtributePostgresql( dimensionName, objSchema);
                }
                else
                {
                    strsql = createSqlForAccessAtributeClickHouse( dimensionName, objSchema);
                }

                strsql = strsql.Replace("?treelevel?", levelId.ToString());

                if(hierarhy_seq!=null){
                    string[] hierarhy_arr = hierarhy_seq.Split("?#?");
                    int len = hierarhy_arr.Length;

                    for(int ind = len-1; ind>=0; ind--){
                        string strDimensionName = hierarhy_arr[ind];
                        string str1 = "?objectName" + (len-ind).ToString() + "?";
                        string str2 = strDimensionName.ToString();
                        strsql = strsql.Replace(str1, str2);

                        memberPatch = string.Format("{0}.[{1}]", memberPatch, str2);
                    }
                }
        
                if(postgresEnable)
                {
                    return executeOlapQueryPostgre(strsql, objSchema, maxIdAttribute, itemRoleName, parentId, levelId, roleName, dimensionName, memberPatch);
                }
                else
                {
                    return executeOlapQueryClickHouse(strsql, objSchema, maxIdAttribute, itemRoleName, parentId, levelId, roleName, dimensionName, memberPatch);
                }


            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + posError);
            }

            return new List<TreeListNode>();
        }

        //Получить список атрибутов для заданного уровня ( вызывается при смене дименшена, возвращает строку JSON )
        public object GetHierarchyAttributeOlapTreeJson(string parentId, byte levelId, string dimensionName, string hierarhy_seq, int maxIdAttribute, string shemaFileName, string roleName)
        {
            string json = null;
            try
            {
                int iParentId = (parentId!=null ? int.Parse(parentId) : -1);
                List<TreeListNode> model = null;

                if(hierarhy_seq!=null){
                    string[] hierarhy_arr = hierarhy_seq.Split("?#?");
                    int len = hierarhy_arr.Length;

                }
            
                if(dimensionName==null){ 
                    model = new List<TreeListNode>();
                }
                else {
                    model =  (List<TreeListNode>)getListAttributeOlapTreeForLevel(iParentId, levelId, dimensionName, hierarhy_seq, maxIdAttribute, shemaFileName, roleName);
                
                }
 
                json = JsonConvert.SerializeObject(model);
            }
            catch(Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.ExceptionErrorMessage() + "  && GetHierarchyAttributeOlapTree, 1" });
                //return BadRequest("Ошибка! Исключение при считывании данных. ", ex);
            }

            return json;

        }

        //ПОДГРУЗИТЬ УЗЕЛ
        //Получить список атрибутов для заданного уровня ( вызывается  раскрытии узла)
        public Object GetHierarchyAttributeOlapTree(string parentId, byte levelId, string dimensionName, string hierarhy_seq, int maxIdAttribute, string shemaFileName, string roleName)
        {
            JsonResult myResult;

            try
            {
                int iParentId = (parentId!=null ? int.Parse(parentId) : -1);

                

                if(dimensionName==null){ 
                    myResult = new JsonResult(new List<TreeListNode>());
                }
                else {
                    myResult =  new JsonResult(getListAttributeOlapTreeForLevel(iParentId, levelId, dimensionName, hierarhy_seq, maxIdAttribute, shemaFileName, roleName));
                }
            }
            catch(Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = "Error! " + ex.ExceptionErrorMessage() + "  && GetHierarchyAttributeOlapTree, 1" });
            }

            return myResult;
        }

        //Получить иерархию заданного измерения
        private List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> getListHierarchy(Schema objSchema, string roleName, string dimensionName)
        {
            //Ищем нужную роль
            SchemaRole shemaRole = null;
            foreach (SchemaRole itemShemaRole in objSchema.Role.ToList()){ 
               
                if(!itemShemaRole.name.Equals(roleName) ){ continue;}
                shemaRole = itemShemaRole;
                break; 
            }
            
            if(shemaRole == null) return null;  // не нашли роль

            //Ищем иерархию в кубе
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLres = null;
            SchemaRoleSchemaGrantCubeGrant currentCubeGrant = null;
            foreach (SchemaRoleSchemaGrantCubeGrant itemCubeGrant in shemaRole.SchemaGrant[0].CubeGrant){ 

                listHierarchyXMLres = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();
                foreach (var resultHierarchy in itemCubeGrant.HierarchyGrant.ToList().Where(s => s.hierarchy == dimensionName))
                {
                    listHierarchyXMLres.Add(resultHierarchy);
                }
                if(listHierarchyXMLres.Count == 0)
                { 
                    continue; 
                }
                return listHierarchyXMLres;
            }

            // Такого дименшена не найдено ни в одном кубе
            return null;

        }

        // Определить, какой доступ у заданного узла
        private bool getMemberAccess(Schema objSchema, string roleName, string dimensionName, string memberPatch){ 
            
            //Получаем иерархию для данной роли и данного дименшена
            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLres =  getListHierarchy(objSchema, roleName, dimensionName);
            
            if(listHierarchyXMLres == null) //Не найдено роли или не найдено такого дименшена ни в одном кубе
            {
                return false;
            }

            if(listHierarchyXMLres.Count == 0)  //Не найдено иерархии
            { 
                return false; 
            }


            if(listHierarchyXMLres.Count == 1)
            { 
                if(listHierarchyXMLres[0].access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }

            if(listHierarchyXMLres.Count > 1){
                bool accessGen = false;
                bool accessTop = false;
                bool flagTop = false;

                bool accessMy = false;
                bool flagMy = false;


                        
                foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyItem in listHierarchyXMLres)
                {
                            

                    if(hierarchyItem.access != SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                    { 
                        if(hierarchyItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all)
                        {
                            accessGen = true;
                        }
                        else if(hierarchyItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none)
                        {
                            accessGen = false;
                        }

                        continue; 
                    }

                    if(hierarchyItem.MemberGrant == null)
                    { 
                        continue;
                    }

                    //Ищем свой
                    foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant memberItem in hierarchyItem.MemberGrant)
                    {
                        if(memberItem.member == memberPatch)
                        { 
                            if(memberItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all )
                            {
                                accessMy =  true;
                            }
                            else
                            {
                                accessMy =  false;
                            }
                            //flagMy = true;
                            if(accessMy == true) return true;


                            //Доступ нижнего уровня (только если есть доступ all)
                            foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant memberItemCh in hierarchyItem.MemberGrant)
                            {
                                if(memberItemCh.member.Contains(memberPatch))
                                { 
                                    if(memberItemCh.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all )
                                    {
                                        return true;
                                        //accessMy = true;
                                        //break;
                                    }
                                    //else
                                    //{
                                    //    return false;
                                    //}
                                    
                                }

                            }

                            return false;
                        }
                    }

                    //НЕТ СВОЕГО

                    //Доступ нижнего уровня (только если есть доступ all)
                    foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant memberItemBt in hierarchyItem.MemberGrant)
                    {
                        if(memberItemBt.member.Contains(memberPatch))
                        { 
                            if(memberItemBt.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all )
                            {
                                return true;
                            }
                                    
                        }

                    }

                    //Доступ верхнего уровня
                    foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant memberItem in hierarchyItem.MemberGrant)
                    {
                        if(memberPatch.Contains(memberItem.member))
                        {
                            if(memberItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none )
                            {
                                accessTop = false;
                                flagTop = true;
                            }
                            else
                            {
                                accessTop = true;
                                flagTop = true;                                       
                            }

                        }

                    }


                    if(flagTop == true)
                    {
                        return accessTop;
                    }

                    // 555555555555555555555
                    /*
                    //Доступ своего уровня
                    foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant memberItem in hierarchyItem.MemberGrant)
                    {
                        if(memberItem.member == memberPatch)
                        { 
                            if(memberItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all )
                            {
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                                    
                        }

                    }
                            
                    //Доступ нижнего уровня (только если есть доступ all)
                    foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant memberItem in hierarchyItem.MemberGrant)
                    {
                        if(memberItem.member.Contains(memberPatch))
                        { 
                            if(memberItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all )
                            {
                                accessTop = true;
                                flagTop = true;
                            }
                            //else
                            //{
                            //    return false;
                            //}
                                    
                        }

                    }


                    //Доступ верхнего уровня
                    foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant memberItem in hierarchyItem.MemberGrant)
                    {
                        if(memberPatch.Contains(memberItem.member))
                        {
                            if(memberItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none )
                            {
                                accessTop = false;
                                flagTop = true;
                            }
                            else
                            {
                                accessTop = true;
                                flagTop = true;                                       
                            }

                        }

                    }
                    if(flagTop == true)
                    {
                        return accessTop;
                    }
                    */

                }

                return accessGen;
                        

            }
            
            return false;
        }

        /*
        private void fillMapMemberPathAccessByRole(Schema objSchema){ 
            
            
        }
        */


        //Сохранение изменений атрибутов (POST запрос)  ActionResult
        [HttpPost]
        public string SaveAttributeOlapPost(string dimensionName, string shemaFileName, string roleName, string jsonStr)
        {
            string str = "";
            List<TreeListNode> model = null;
            Schema objSchema = null;
            int countCheck = 0;

            try
            {
                str = jsonStr;
                model = JsonConvert.DeserializeObject<List<TreeListNode>>(jsonStr); 


                //ФОРМИРОВАНИЕ СПИСКОВ ИЗМЕНЕНИЙ  
                List<string> listSelFalse =  new List<string>();
                List<string> listSelTrue =  new List<string>();
                List<string> addListSelFalse =  new List<string>();
                List<string> addListSelTrue =  new List<string>();

                foreach(TreeListNode item in model)
                {
                    while(item.NodeName.Contains("##XX##"))
                    {
                        item.NodeName = item.NodeName.Replace("##XX##", "].[");
                    }
                    item.NodeName = "[" + item.NodeName + "]";
                    
                    bool flagAddFalse = true;
                    bool flagAddTrue = true;
                    if(item.selected == false){ 
                        if(listSelFalse.Count == 0)
                        { 
                            listSelFalse.Add(item.NodeName);
                        } 
                        else 
                        {
                            for(int ind = 0; ind < listSelFalse.Count; ind++)
                            {
                                string lstitem = listSelFalse[ind];
                                if(item.NodeName.Contains(lstitem))
                                {
                                    flagAddFalse = false;
                                    continue;
                                }
                                else if(lstitem.Contains(item.NodeName))
                                {
                                    listSelFalse[ind] = item.NodeName;
                                    flagAddFalse = false;
                                }
                                
                            }

                            if(flagAddFalse)
                            { 
                                listSelFalse.Add(item.NodeName);
                            }

                        }
                        
                    }
                    else
                    { //item.selected == true
                        if(listSelTrue.Count == 0)
                        { 
                            listSelTrue.Add(item.NodeName);
                        } 
                        else 
                        {
                            for(int ind = 0; ind < listSelTrue.Count; ind++)
                            {
                                string lstitem = listSelTrue[ind];
                                if(lstitem.Contains(item.NodeName))
                                {
                                    flagAddTrue = false;
                                    continue;
                                }
                                else if(item.NodeName.Contains(lstitem))
                                {
                                    listSelTrue[ind] = item.NodeName;
                                    flagAddTrue = false;
                                }
                                
                            }

                            if(flagAddTrue)
                            { 
                                listSelTrue.Add(item.NodeName);
                            }

                        }
                    }


                    
                }

                //ПРИМЕНЕНИЕ ИЗМЕНЕНИЙ К МЕМБЕРАМ И СОЗДАНИЕ ЭТАЛОНА

                //Ищем первый дименшен для заданной роли
                HttpContext.Session.SetObjectAsJson("ShemaFileSelected", shemaFileName);
                objSchema = getShema();
                if(objSchema == null)
                {
                    return "Сохранение не выполнено. Не удалось загрузить файл с OLAP-схемой";
                }

                countCheck = checkRoleXML(false, ref objSchema);

                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLres =  getListHierarchy(objSchema, roleName, dimensionName);

                //Не найдено ни одной иерархии - выходим
                if(listHierarchyXMLres == null) 
                {
                    return "Сохранение не выполнено. Для заданной роли дименшен не входит ни в один куб";

                }
                if(listHierarchyXMLres.Count == 0) return "Сохранение не выполнено. Для заданной роли дименшен не входит ни в один куб";

                bool accessGen = false;	
                bool accessFull = false;
                if(listHierarchyXMLres.Count == 1)
                {
                    accessFull = true; //Полный доступ/запрет
                }

                SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantCustom = null;
                SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantGen = null;

                foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyItem in listHierarchyXMLres)
                {
                    if(hierarchyItem.access != SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                    { 
                        hierarchyGrantGen = hierarchyItem;
                        if(hierarchyItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all)
                        {
                            accessGen = true;
                        }
                        else if(hierarchyItem.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none)
                        {
                            accessGen = false;
                        }
                    } 
                    else 
                    {
                        //hierarchyGrantCustom = hierarchyItem;

                        hierarchyGrantCustom = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant
                                                { 
                                                    hierarchy = hierarchyItem.hierarchy,
                                                    access = hierarchyItem.access,
                                                    rollupPolicy = hierarchyItem.rollupPolicy
                                                };
                        //-------------
                        if(hierarchyItem.MemberGrant != null)
                        {
                            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> lstMemberGrant = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                            foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant srcItem in hierarchyItem.MemberGrant)
                            {

                                 SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                                    {
                                        //member = lstitem,
                                        member = srcItem.member, 
                                        access = srcItem.access
                                    };
                                lstMemberGrant.Add(objMember);

                            }
                            hierarchyGrantCustom.MemberGrant = lstMemberGrant.ToArray();

                        }




                        //---------

                    }
                }

                //Нет общей иерархии, выходим
                if(hierarchyGrantGen == null) return "Сохранение не выполнено. Для выбранного дименшена отсутствует иерархия с доступом all или none"; //();  // #### сделать сообщение об ошибке


                //LIST_FALSE
                if(accessFull)
                {
                    //Если полный доступ, добавляем мембер
                    if(!accessGen)
                    { 
                        //Добавляем HierarchyGrant custom
                        hierarchyGrantCustom = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant
                        { 
                            hierarchy = dimensionName,
                            access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom,
                            rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial
                        };
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listObjMember = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                        foreach(string lstitem in listSelTrue)
                        {
                            SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                            {
                                member =  string.Format("[{0}].{1}", dimensionName, lstitem), 
                                access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all
                            };
                            listObjMember.Add(objMember);
                            accessFull = false;
                        }
                        hierarchyGrantCustom.MemberGrant = listObjMember.ToArray();
                    }
                    else
                    {  // accessGen = true

                        //Если полный доступ, добавляем мембер
                        //Добавляем HierarchyGrant custom
                        hierarchyGrantCustom = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant
                        { 
                            hierarchy = dimensionName,
                            access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom,
                            rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial
                        };
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listObjMember = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                        foreach(string lstitem in listSelFalse)
                        {
                            SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                            {
                                //member = lstitem,
                                member =  string.Format("[{0}].{1}", dimensionName, lstitem),
                                access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none
                            };
                            listObjMember.Add(objMember);
                            accessFull = false;
                        }
                        hierarchyGrantCustom.MemberGrant = listObjMember.ToArray();


                    }

                }
                else
                { // !accessFull

                    //FALSE
                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listObjMemberForDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                    foreach(string lstitem in listSelFalse)
                    {
                        //string lstitem = listSelFalse[ind];
                        bool flNoAddItem = false;
                        foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMember in hierarchyGrantCustom.MemberGrant)
                        {
                            if(itemMember.member == lstitem && itemMember.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none )
                            {
                                listObjMemberForDelete.Add(itemMember);
                                flNoAddItem = true;
                            }
                            else if(itemMember.member.Contains(lstitem))
                            {
                                listObjMemberForDelete.Add(itemMember);
                                //flNoAddItem = true;
                            }
                        }
                        //если нет ни одного удаления. скорей всего надо добавить  ####
                        //если флаг сброшен, нужно добавить мембер
                        if(flNoAddItem == false)
                        {
                            SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                            {
                                member =  string.Format("[{0}].{1}", dimensionName, lstitem), 
                                access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none
                            };

                            List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> lstMemberGrantF = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>(hierarchyGrantCustom.MemberGrant.ToList());
                            lstMemberGrantF.Add(objMember);
                            hierarchyGrantCustom.MemberGrant = lstMemberGrantF.ToArray();
                        }

                        
                    }

                    if(listObjMemberForDelete.Count > 0)
                    {
                        List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> lstMemberGrantD = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>(hierarchyGrantCustom.MemberGrant.ToList());
                        foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMember in listObjMemberForDelete)
                        {
                           lstMemberGrantD.Remove(itemMember);
                        }
                        hierarchyGrantCustom.MemberGrant = lstMemberGrantD.ToArray();
                    }

                    // ................................................
                    //TRUE

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> lstMemberGrant = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>(hierarchyGrantCustom.MemberGrant.ToList());
                    foreach(string lstitem in listSelTrue)
                    {

                         SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                            {
                                //member = lstitem,
                                member =  string.Format("[{0}].{1}", dimensionName, lstitem), 
                                access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all
                            };
                        lstMemberGrant.Add(objMember);

                    }
                    hierarchyGrantCustom.MemberGrant = lstMemberGrant.ToArray();

                }
 


                //Если мемберов нет, то hierarchyGrantCustom ставим в null
                if(hierarchyGrantCustom.MemberGrant == null || hierarchyGrantCustom.MemberGrant.Length == 0)
                {
                    hierarchyGrantCustom = null;
                }

                //ОПТИМИЗАЦИЯ hierarchyGrantCustom
                optimizationHierarchyGrantCustom(objSchema, dimensionName, shemaFileName, roleName, ref hierarchyGrantGen, ref hierarchyGrantCustom);

                

                //БАЛАНСИРОВКА МЕЖДУ ПОЛНЫМ ДОСТУПОМ И ПОЛНЫМ ЗАПРЕТОМ

                //ПРИМЕНЕНИЕ ЭТАЛОНА КО ВСЕМ КУБАМ И СОХРАНЕНИЕ СХЕМЫ
                //SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantCustom = null;
                //SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantGen = null;

                applyСhangesAccessAttribute(hierarchyGrantGen, hierarchyGrantCustom, objSchema, dimensionName, shemaFileName, roleName);

//@@@
            }
            catch (Exception ex)
            {
                return "Сохранение не выполнено. Произошло исключение с ошибкой " + ex.Message; //BadRequest(null, ex);
            }

            return "OK";
        }


        
        private void checkNodeTree(
                                    int iParentId, int levelId, 
                                    string dimensionName, string hierarhy_seq, 
                                    string shemaFileName, string roleName,
                                    string parentMember,
                                    MondrianNodeInfo nodeInfo,
                                    ref Dictionary<string, int> dictMember,
                                    ref Dictionary<string, MondrianNodeInfo> dictNodeInfo,
                                    ref SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantGen, 
                                    ref SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantCustom
                                    )
        {

            List<TreeListNode> model = (List<TreeListNode>)getListAttributeOlapTreeForLevel(iParentId, (byte)levelId, dimensionName, hierarhy_seq, 1, shemaFileName, roleName);

            int lenMember = 0;
            int accessTop = 0;
            bool flagCalcTopAccess = false;

            foreach(TreeListNode itemModel in model)
            {
                
                string memberModel = parentMember == null ? string.Format("[{0}].[{1}]", dimensionName, itemModel.NodeName)
                                                            : string.Format("{0}.[{1}]", parentMember, itemModel.NodeName); 
                if(dictMember.ContainsKey(memberModel))
                {
                    if(memberModel == nodeInfo.Member)
                    {
                        dictNodeInfo.Add(nodeInfo.Member, nodeInfo);
                        //continue;
                    }
                            
                } 
                else
                {
                    bool flSkip = false;
                    //Есть ли потомки данного мембера ниже? Если есть, пропускаем.
                    foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant item in hierarchyGrantCustom.MemberGrant)
                    {
                        if(item.member.Contains(memberModel))
                        {
                            flSkip = true;
                        }
                    }

                    if(flSkip) continue;

                    //Ищем ближайший верхний
                    if(!flagCalcTopAccess)
                    {
                        foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant item in hierarchyGrantCustom.MemberGrant)
                        {
                            if(parentMember == null) continue;
                            if(parentMember.Contains(item.member))
                            {
                                if(item.member.Length > lenMember)
                                {
                                    lenMember = item.member.Length;
                                    accessTop = item.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all ? 1 : 0;
                                    flagCalcTopAccess = true;
                                }
                            }
                        }
                        //Если ничего не нашли берем Gen доступ
                        if(!flagCalcTopAccess)
                        {
                            accessTop = hierarchyGrantGen.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all ? 1 : 0;
                            flagCalcTopAccess = true;
                        }
                    }

                    MondrianNodeInfo nodeInfoModel = new MondrianNodeInfo
                    {
                        Member = memberModel,
                        ParentMember = parentMember,
                        childrenCount = 1,
                        childrenCountAll = accessTop == 0 ? 0 : 1
                    };

                    if(!dictMember.ContainsKey(memberModel))
                    {
                        dictMember.Add(memberModel, accessTop == 0 ? 0 : 1);
                    }

                    dictNodeInfo.Add(memberModel, nodeInfoModel);
                }

            }


        }

        string getHierarhy_seq(string strMbr, int levelFromEnd)
        {
            string tmp = strMbr.Trim(new Char[] { '[', ']'}); 
            string[] arrNode = tmp.Split("].[");
                
            if(arrNode.Length-levelFromEnd-1 <= 0) return null;

            string hierarhy_seq =  string.Format("{0}", arrNode[arrNode.Length-levelFromEnd-1]);
            int levelId = arrNode.Length-levelFromEnd;
            if(arrNode.Length-levelFromEnd-2 <= 0) return hierarhy_seq;

            // GMOSE162?#?ТЭЦ-20?#?Филиал ТЭЦ-20 "Мосэнерго"?#?ПАО "Мосэнерго"
            for(int ind = arrNode.Length-levelFromEnd-2; ind>0; ind--)
            {
                hierarhy_seq = string.Format("{0}?#?{1}", hierarhy_seq, arrNode[ind]);
            }
            return hierarhy_seq;
        }

        string getParentMember(string strMbr, int levelFromEnd)
        {
            string tmp = strMbr.Trim(new Char[] { '[', ']'}); 
            string[] arrNode = tmp.Split("].[");
            string parentMember = string.Format("[{0}]", arrNode[0]);
            if(arrNode.Length-1 <= levelFromEnd) return null;

            for(int ind = 1; ind<arrNode.Length-levelFromEnd; ind++)
            {
                parentMember = string.Format("{0}.[{1}]", parentMember, arrNode[ind]);
            }
            return parentMember;
        }


        private bool optimizationHierarchyGrantCustom(Schema objSchema, string dimensionName, string shemaFileName, string roleName, ref SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantGen, ref SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantCustom)
        {
            if(hierarchyGrantCustom == null) return true;
            List<string> listMember =  new List<string>();
            Dictionary<string, int> dictMember =  new  Dictionary<string, int>();
            
            //Формирование списка мемберов и мапки мембер-доступ
            foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant item in hierarchyGrantCustom.MemberGrant)
            {
                int iSelected = item.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all ? 1:0; 
                dictMember.Add(item.member, iSelected);

                bool flagAddTrue = true;
                if(listMember.Count == 0)
                { 
                    listMember.Add(item.member);
                } 
                else 
                {
                    for(int ind = 0; ind < listMember.Count; ind++)
                    {
                        string lstitem = listMember[ind];
                        if(lstitem.Contains(item.member))
                        {
                            flagAddTrue = false;
                            continue;
                        }
                        else if(item.member.Contains(lstitem))
                        {
                            listMember[ind] = item.member;
                            flagAddTrue = false;
                        }
                                
                    }

                    if(flagAddTrue)
                    { 
                        listMember.Add(item.member);
                    }

                }
            }


            Dictionary<string, MondrianNodeInfo> dictNodeInfo = new Dictionary<string, MondrianNodeInfo>();

            int iParentId = 1; 
            int levelId = 1;
            string hierarhy_seq;
            //, 1, shemaFileName, roleName

            foreach(string strMbr in listMember)
            {

                string tmp = strMbr.Trim(new Char[] { '[', ']'}); 
                string[] arrNode = tmp.Split("].[");
                levelId = arrNode.Length-1;

                /*
                string tmp = strMbr.Trim(new Char[] { '[', ']'}); 
                string[] arrNode = tmp.Split("].[");
                
                hierarhy_seq =  string.Format("{0}", arrNode[arrNode.Length-2]);
                levelId = arrNode.Length-1;
                // GMOSE162?#?ТЭЦ-20?#?Филиал ТЭЦ-20 "Мосэнерго"?#?ПАО "Мосэнерго"
                for(int ind = arrNode.Length-3; ind>0; ind--)
                {
                    hierarhy_seq = string.Format("{0}?#?{1}", hierarhy_seq, arrNode[ind]);
                }
                */
                int levelFromEnd = 1;
                hierarhy_seq = getHierarhy_seq(strMbr, levelFromEnd);
                string parentMember = getParentMember(strMbr, levelFromEnd);

                MondrianNodeInfo nodeInfo = new MondrianNodeInfo
                {
                    Member = strMbr,
                    ParentMember = parentMember,
                    childrenCount = 1,
                    childrenCountAll = dictMember[strMbr] == 0 ? 0 : 1
                };

                bool flExists = false;
                foreach (KeyValuePair<String, MondrianNodeInfo> entry in dictNodeInfo)
                {
                    MondrianNodeInfo nodeInfoDict = entry.Value;
                    if(nodeInfoDict.ParentMember == nodeInfo.ParentMember)
                    {
                        flExists = true;
                        break;
                    }
                }

                /*
                //Самый верхний уровень
                if(hierarhy_seq == null && parentMember == null)
                {
                    flExists = true;

                    checkNodeTree(
                                    iParentId, 1, 
                                    dimensionName, hierarhy_seq, 
                                    shemaFileName, roleName,
                                    parentMember,
                                    nodeInfo,
                                    ref dictMember,
                                    ref dictNodeInfo,
                                    ref hierarchyGrantGen, 
                                    ref hierarchyGrantCustom
                                    );
                }
                */
                if(!flExists)
                {
                    //Eсли парента еще нет в nodeInfoDict, то проверяем дочерние узлы

                    //while(hierarhy_seq != null && parentMember != null)
                    while(levelId>0)
                    {

                        checkNodeTree(
                                    iParentId, levelId, 
                                    dimensionName, hierarhy_seq, 
                                    shemaFileName, roleName,
                                    parentMember,
                                    nodeInfo,
                                    ref dictMember,
                                    ref dictNodeInfo,
                                    ref hierarchyGrantGen, 
                                    ref hierarchyGrantCustom
                                    );

                        levelFromEnd++;
                        hierarhy_seq = getHierarhy_seq(strMbr, levelFromEnd);
                        parentMember = getParentMember(strMbr, levelFromEnd);
                        levelId--;


                    }
                } 
                else
                {
                    dictNodeInfo.Add(nodeInfo.Member, nodeInfo);
                }
            }

            int size = dictNodeInfo.Count;
            int cntTotal = 0;
            int cntAll = 0;
            int cntNone = 0;
            List<string> testMember = new List<string>();
            foreach (KeyValuePair<String, MondrianNodeInfo> entry in dictNodeInfo)
            {
                MondrianNodeInfo nodeInfo = entry.Value;
                cntTotal += nodeInfo.childrenCount;
                cntAll += nodeInfo.childrenCountAll;

                testMember.Add(entry.Key + "_" + nodeInfo.childrenCountAll);
            }
            cntNone = cntTotal - cntAll;
            if(cntAll>cntNone)
            {
                //Gen = all, member = none
                hierarchyGrantGen.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all;
                if(cntNone == 0)
                {
                    hierarchyGrantCustom = null;
                }
                else
                {
                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> lstMemberGrant = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                    foreach (KeyValuePair<String, MondrianNodeInfo> entry in dictNodeInfo)
                    {
                        MondrianNodeInfo nodeInfo = entry.Value;
                        if(nodeInfo.childrenCountAll == 0)
                        {
                             SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                                    {
                                        //member = lstitem,
                                        member = nodeInfo.Member, 
                                        access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.none
                                    };
                             lstMemberGrant.Add(objMember);
                        }

                    }
                    hierarchyGrantCustom.MemberGrant = lstMemberGrant.ToArray();
                }
            }
            else
            {
                //Gen = none, member = all
                hierarchyGrantGen.access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none;
                if(cntAll == 0)
                {
                    hierarchyGrantCustom = null;
                }
                else
                {
                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> lstMemberGrant = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                    foreach (KeyValuePair<String, MondrianNodeInfo> entry in dictNodeInfo)
                    {
                        MondrianNodeInfo nodeInfo = entry.Value;
                        if(nodeInfo.childrenCountAll == 1)
                        {
                             SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant objMember = new SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant
                                    {
                                        member = nodeInfo.Member, 
                                        access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrantAccess.all
                                    };
                             lstMemberGrant.Add(objMember);
                        }

                    }
                    hierarchyGrantCustom.MemberGrant = lstMemberGrant.ToArray();
                }

            }

            if(hierarchyGrantCustom != null)
            {
                int cntCustom = hierarchyGrantCustom.MemberGrant.Length;
            }

            return true;
        }




        private bool applyСhangesAccessAttribute(SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantGen, SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantCustom, Schema objSchema, string dimensionName, string shemaFileName, string roleName)
        {
            int countCheck = 0;
            if(objSchema == null)
            {
                
                HttpContext.Session.SetObjectAsJson("ShemaFileSelected", shemaFileName);
                objSchema = getShema();
                if(objSchema == null) return false;
                countCheck = checkRoleXML(false, ref objSchema);
            }

            //Ищем нужную роль
            SchemaRole shemaRole = null;
            foreach (SchemaRole itemShemaRole in objSchema.Role.ToList()){ 
               
                if(!itemShemaRole.name.Equals(roleName) ){ continue;}
                shemaRole = itemShemaRole;
                break; 
            }
            
            if(shemaRole == null) return false;  // не нашли роль  ####

                                

            //Ищем иерархию в кубе
            //List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarchyXMLres = null;
            bool flagContainsTotal = false;
            SchemaRoleSchemaGrantCubeGrant currentCubeGrant = null;
            foreach (SchemaRoleSchemaGrantCubeGrant itemCubeGrant in shemaRole.SchemaGrant[0].CubeGrant)
            { 
                bool flagContains = false;
                if(dictDimensionByCubeXML.ContainsKey(itemCubeGrant.cube))
                {
                    Dictionary<String, String> dictHierarhy = dictDimensionByCubeXML[itemCubeGrant.cube];
                    if(dictHierarhy.ContainsKey(dimensionName))
                    {
                        flagContains = true;
                        flagContainsTotal = true;
                    }
                }
                else if(dictDimensionByVirtualCubeXML.ContainsKey(itemCubeGrant.cube))
                {
                    Dictionary<String, String> dictHierarhy = dictDimensionByVirtualCubeXML[itemCubeGrant.cube];
                    if(dictHierarhy.ContainsKey(dimensionName))
                    {
                        flagContains = true;
                        flagContainsTotal = true;
                    }
                }

                if(!flagContains) continue;

                bool flagAllNone = false;	
                bool flagCustom = false;
                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarhyForDelete = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();

                foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemHierarhy in itemCubeGrant.HierarchyGrant)
                {
                    if(itemHierarhy.hierarchy == dimensionName)
                    {
                        if(itemHierarhy.access == SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.custom)
                        {
                            flagCustom = true;

                            if(hierarchyGrantCustom == null) 
                            {
                                listHierarhyForDelete.Add(itemHierarhy);
                            }
                            else
                            {
                                if(itemHierarhy.MemberGrant!=null)
                                {
                                    Array.Clear(itemHierarhy.MemberGrant, 0, itemHierarhy.MemberGrant.Length);
                                }
                                

                                if(hierarchyGrantCustom.MemberGrant == null) continue;
                                if(hierarchyGrantCustom.MemberGrant.Length == 0) continue;
                                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listMember = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                                foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMember in hierarchyGrantCustom.MemberGrant)
                                {
                                    listMember.Add(itemMember);
                                    //itemHierarhy.MemberGrant.Append(itemMember);
                                }
                                itemHierarhy.MemberGrant = listMember.ToArray();
                            }
                        }
                        else
                        {
                            flagAllNone = true;
                            itemHierarhy.access = hierarchyGrantGen.access;
                        }

                    }
                }

                //Если в схеме нет иерархии custom, а в эталоне он есть, то добавляем
                if(!flagCustom && hierarchyGrantCustom!=null && hierarchyGrantCustom.MemberGrant!=null)
                {
                    SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantCustomNew = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant
                    { 
                        hierarchy = hierarchyGrantCustom.hierarchy,
                        access = hierarchyGrantCustom.access,
                        rollupPolicy = hierarchyGrantCustom.rollupPolicy
                    };

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant> listMember = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant>();
                    foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrantMemberGrant itemMember in hierarchyGrantCustom.MemberGrant)
                    {
                        listMember.Add(itemMember);
                    }
                    hierarchyGrantCustomNew.MemberGrant = listMember.ToArray();

                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarhy =  itemCubeGrant.HierarchyGrant.ToList();
                    listHierarhy.Add(hierarchyGrantCustomNew);
                    itemCubeGrant.HierarchyGrant = listHierarhy.ToArray();

                }

                /*
                if(listHierarhyForDelete.Count>0)
                {
                    List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarhy =  itemCubeGrant.HierarchyGrant.ToList();
                    listHierarhy.Remove(listHierarhyForDelete);
                    itemCubeGrant.HierarchyGrant = listHierarhy.ToArray();
                }
                */

                List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant> listHierarhyD =  itemCubeGrant.HierarchyGrant.ToList();
                foreach(SchemaRoleSchemaGrantCubeGrantHierarchyGrant itemDel in listHierarhyForDelete)
                {
                    listHierarhyD.Remove(itemDel);
                }
                itemCubeGrant.HierarchyGrant = listHierarhyD.ToArray();
                
                //listHierarchyXMLres = new List<SchemaRoleSchemaGrantCubeGrantHierarchyGrant>();

            }

            if(!flagContainsTotal) 
            {
                throw new Exception("Сохранение не выполнено. Для заданной роли дименшен не входит ни в один куб");
                //return true; // Дименшен в кубах отсутствует 
            }

            countCheck = checkRoleXML(false, ref objSchema);
            //Сохраняем
            schemaSave(objSchema);
            return true;
        }

        //SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantCustom = null;
        //SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantGen = null;
        public string SetFullAccessAllToAttribute(string parentId, byte levelId, string dimensionName, int maxIdAttribute, string shemaFileName, string roleName)
        {
            try{
                SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantGen = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant
                                                    { 
                                                        hierarchy = dimensionName,
                                                        access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.all,
                                                        rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial
                                                    };

                bool bres = applyСhangesAccessAttribute(hierarchyGrantGen, null, null, dimensionName, shemaFileName, roleName);
                if(!bres)
                {
                    return  "Сохранение не выполнено. Для заданной роли дименшен не входит ни в один куб";

                }
            }
            catch (Exception ex)
            {
                return ex.Message; //BadRequest(null, ex);
            }
            return "OK";
        }

        public string SetFullAccessNoneToAttribute(string parentId, byte levelId, string dimensionName, int maxIdAttribute, string shemaFileName, string roleName)
        {
            try{
                SchemaRoleSchemaGrantCubeGrantHierarchyGrant hierarchyGrantGen = new SchemaRoleSchemaGrantCubeGrantHierarchyGrant
                                                    { 
                                                        hierarchy = dimensionName,
                                                        access = SchemaRoleSchemaGrantCubeGrantHierarchyGrantAccess.none,
                                                        rollupPolicy = SchemaRoleSchemaGrantCubeGrantHierarchyGrantRollupPolicy.partial
                                                    };

                bool bres = applyСhangesAccessAttribute(hierarchyGrantGen, null, null, dimensionName, shemaFileName, roleName);
                if(!bres)
                {
                    return  "Сохранение не выполнено. Для заданной роли дименшен не входит ни в один куб";

                }
            }
            catch (Exception ex)
            {
                return ex.Message; //BadRequest(null, ex);
            }
            return "OK";
        }




        /*
        public object SaveAttributeOlap(string dimensionName, string shemaFileName, string roleName, string items)
        {
            string str = items;
            List<TreeListNode> model = null;

            try
            {

                model = JsonConvert.DeserializeObject<List<TreeListNode>>(items); 


            }
            catch (Exception ex)
            {
                return BadRequest(null, ex);
            }

            return "OK";
            //IEnumerable<Dictionary<string, object>> model = ToDataTableList(result);
            //return DataSourceLoader.Load(model, loadOptions);
        }
        */

        //Число потомков у данного узла
        public string GetSizeСhildrenAttribute(string parentId, byte levelId, string dimensionName, string hierarhy_seq, string shemaFileName, string roleName)
        {
            string json = null;
            int iParentId = (parentId!=null ? int.Parse(parentId) : -1);
            List<TreeListNode> model = null;
            model =  (List<TreeListNode>)getListAttributeOlapTreeForLevel(iParentId, levelId, dimensionName, hierarhy_seq, 1, shemaFileName, roleName);
            
            int sizeParent = model.Count;
            return sizeParent.ToString();
        }

    }
}
