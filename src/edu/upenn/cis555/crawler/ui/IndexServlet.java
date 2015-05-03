package edu.upenn.cis555.crawler.ui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class IndexServlet extends HttpServlet {

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		if (request.getServletPath().equals("/")) {
			String html = "";
			html += "<html>";
			html += "<head>";
			html += "<link rel='stylesheet' type='text/css' "
					+ "href='//maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css'>";
			html += "</head>";
			html += "<body>";
			html += "<div class='container'>";
			html += "<h2>Hype Engine Admin Crawler Panel</h2>";
			html += "</div>";
			html += "</body>";
			html += "</html>";
			response.getWriter().print(html);
		}
	}
}

